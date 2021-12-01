from bs4 import BeautifulSoup
import requests
from requests.exceptions import RequestException
import requests_html
from pyppeteer.errors import PyppeteerError
from urllib.parse import urlsplit, urljoin
from datetime import datetime
import os
import json
import sys
import traceback
from collections import deque


def crawler(top_dir: str, input_url: str):
    #exception handler for session.loop
    def custom_exception_handler(loop, context):
        # first, handle with default handler
        loop.default_exception_handler(context)
        #여기부터 원하는 exception 처리
        exception = context.get('exception')
        if isinstance(exception, KeyboardInterrupt):
            print(context)
            loop.stop()

    #global 변수 모음
    #방문한 url 주소를 dictionary로 처리
    #new_main에서 방문한 url과 다운받은 이미지는 True
    #외부링크(블랙리스트)나, 아직 방문하지 않은 링크는 False

    #sys.exit(0)
    visited = {input_url: False}
    #다운받은 이미지 링크 수집:
    downloaded = {}
    #url_frontier를 자료구조로 deque로 처리
    first_dict = dict({'url': input_url, 'img_flag': False})
    url_q = deque([first_dict])
    #requests_html 모듈의 HTMLSession() 기반으로 동작
    session = requests_html.HTMLSession()

    #session.loop

    #url의 domain_label을 얻는다
    #get_label('https://naver.com') -> naver
    def get_label(url) -> str:
        parts = urlsplit(url)
        base_url = "{0.netloc}".format(parts)
        dlist = base_url.split('.')
        if len(dlist) > 2:
            return dlist[1]
        else:
            return dlist[0]

    #url의 domain_url을 얻는다
    #get_label('https://blog.naver.com/phantasmist') -> https://blog.naver.com/
    def get_domain(url) -> str:
        parts = urlsplit(url)
        base_url = "{0.netloc}".format(parts)
        return base_url

    #'main_url'에 http request를 보내고
    #그 response를 받아, html에서 적합한 링크와 이미지를 수집한다
    def new_main(main_url: str, top_dir, update_date, img_flag):
        #requests_html 모듈 기반:
        #persistent session based img_downloader v3
        def img_download3(img_url, filepath):
            with open(filepath, 'wb') as fp:
                try:
                    resp = session.get(img_url, stream=True)
                except RequestException as err:
                    print('RequestException: ', err)
                else:
                    if resp.status_code == 200:
                        try:
                            with open(filepath, 'wb') as fp:
                                for block in resp.iter_content(1024):
                                    if not block:
                                        break
                                    fp.write(block)
                        except Exception:
                            print('img download failed')
                            traceback.print_exc()
                        else:
                            print('img: ', img_url)

        #main_url로 http-request get 보내기
        try:
            resp = session.get(main_url)
        except RequestException as err:
            print('RequestException: ', err)
            return False
        else:
            #RequestException이 발생하지 않았다면 방문여부 표시
            if resp.status_code == 200:
                #print(resp.headers['User-Agent'])
                visited[main_url] = True
                """자바스크립트 렌더링
                wait=0.5 등의 optional arg를 넘길 수 있다
                user-agent, timeout=8 등이 기본으로 적용됨
                javascript=True 같은 arg를 받아 optional 하게 실행하도록 해도 좋아보임
                """
                #resp.html.render()
                soup = BeautifulSoup(resp.text, 'html.parser')
                a_tags = soup.find_all('a')
                for a in a_tags:
                    href = a.attrs.get('href')
                    #urlcheck/ blacklist
                    if href == None:
                        continue
                    if href.startswith(('javascript:', '#', 'mailto:')):
                        continue
                    href = urljoin(main_url, href)
                    # visited check
                    if visited.get(href) == None:
                        if input_url not in href:
                            print('invalid link: ', href)
                            visited[href] = False
                            continue
                        else:
                            print('valid link:', href)
                            visited[href] = False
                            if a.find("img") == None:
                                url_q.append({'url': href, 'img_flag': False})
                            else:
                                url_q.appendleft({
                                    'url': href,
                                    'img_flag': True
                                })

                #img_flag에 따라 이 루프 생략
                #렌더링된 html에서 img 태그 수집
                if img_flag == False:
                    return False
                print("start img collect")

                #img_flage ==True 일때만 서브디렉토리 생성
                #top_dir\top_dir_now\이미지파일들...
                now = datetime.now()
                sub_dir = top_dir + '_' + str(
                    now.strftime('%Y_%m_%d_%H_%M_%S_%f'))
                full_path = os.path.join(top_dir, str(sub_dir))
                print("full path : " + full_path)
                print("top_dir : " + top_dir)
                if not (os.path.isdir(full_path)):
                    os.makedirs(full_path)

                #이미지 다운로드 루프
                imgs = resp.html.find('img')
                if imgs == None:
                    print('no image')
                for idx, img in enumerate(imgs):
                    src = str(img.attrs.get('src'))
                    #'data:'로 시작하는 src는 'data-src' 속성을 갖고 있다면 가져온다
                    if src.startswith('data:'):
                        src = img.attrs.get('data-src')
                    if src != "" and src != None and src != 'None':
                        #relative/absolute url에 전부 대응하도록 url 교정
                        #resp.html._make_absolute(src)의 경우 lxml에서 Exception이 발생하기도 함
                        src = urljoin(main_url, src)
                        #이미지 url에 방문여부 기록
                        #downloaded[src] == True 일 경우 다운로드 받은 이미지이므로 생략
                        if downloaded.get(src) == None:
                            downloaded[src] = False
                        elif downloaded.get(src) == True:
                            continue
                        #모든 경우에 url에서 파일 확장자를 추출할 수 있지는 않다
                        #헤더에서 파일 확장자를 받는 기능은 필요하다면 나중에 구현
                        filename = str(idx) + '.jpg'
                        filepath = os.path.join(full_path, str(filename))
                        #update_date == None(크롤링 기록 X)이면 이미지 다운로드
                        if update_date == None:
                            img_download3(src, filepath)
                        #크롤링한 기록이 있다면 if-modified-since를 헤더에 추가해서
                        #response code가 200이라면 이미지 다운로드
                        else:
                            update = {'If-Modified-Since': update_date}
                            try:
                                resp = session.get(src, headers=update)
                            except RequestException as err:
                                print('RequestException: ', err)
                                continue
                            else:
                                # response code가 200이면 update_date이후에 변경된 것이므로 다운
                                if resp.status_code == 200:
                                    try:
                                        with open(filepath, 'wb') as fp:
                                            for block in resp.iter_content(
                                                    1024):
                                                if not block:
                                                    break
                                                fp.write(block)
                                    except Exception:
                                        print('img download failed')
                                        traceback.print_exc()
                                    else:
                                        print('update: ', src)
                                else:
                                    print("existing img")
                        #이후 동일한 url의 이미지 다운로드를 방지하기 위해 표시
                        downloaded[src] = True

    #while 루프로 url_q가 비거나,
    #exit condition에 도달할 때까지 new_main을 실행
    def url_loop(top_dir, update_date):
        #일단 임시로 제한 낮게 설정해서 전체 프로세스 점검
        IMGLIMIT = 1_000
        URLLIMIT = 1_000
        while (url_q):
            len_url_q = len(url_q)
            len_visited = len(visited)
            len_downloaded = len(downloaded)
            #여기에 exit mechanism 추가 가능
            if sum(x for x in visited.values()) > URLLIMIT:
                print('URLLIMIT reached')
                break
            if len_downloaded > IMGLIMIT:
                print('IMGLIMIT reached')
                break
            url_dict = url_q.popleft()
            print(url_dict)
            main_url = url_dict['url']
            img_flag = url_dict['img_flag']
            print('len(url_q): ', len_url_q)
            print('len(visited): ', len_visited)
            #수집한 url - 빙문할 url(url_q) = 처리된 url
            print('main_url: ', main_url)
            new_main(main_url, top_dir, update_date, img_flag)
        #exit 통계
        no_visited = sum(x for x in visited.values())
        print('no. of visited urls: ', no_visited)
        no_downloaded = sum(x for x in downloaded.values())
        print('no. of downloaded imgs: ', no_downloaded)
        print('Crawling comeplete!')
        return True

    #이전에 crawling 기록이 있는지 json 파일에서 찾아 본 후
    #json 파일이 없다면 파일 생성 후 오늘 날짜 기록
    #json 파일이 있다면 과거 기록을 update_date에 반영하고 json 파일 수정
    #마지막으로 while_loop 함수를 실행해서 본격적인 크롤링을 시작한다
    def start_crawler(input_url, top_dir):
        #top_dir 폴더명 추출 겸 domain 추출
        #사용자 input값을 받도록 수정
        #top_dir = get_label(input_url)
        update_date = None
        json_record = {}
        #http 요청을 보낸 후 오늘 날짜 받아온다...
        try:
            temp_resp = session.get(input_url)  #url arg 있는데?
        except RequestException as err:
            print('RequestException: ', err)
            return False
        else:
            if temp_resp.status_code == 200:
                today = temp_resp.headers["Date"]
                json_record[input_url] = today
                print(today)
            else:
                print(f"Http Error Code : {temp_resp.status_code}")
                return False
            log_filename = './log_file.json'
            #json 형식의 로그파일이 없으면, json_record를 log_file.json에 작성
            if not (os.path.exists(log_filename)):
                with open(log_filename, 'w') as f:
                    f.write(json.dumps(json_record, indent='\t'))
            #json 형식의 로그파일 존재시..
            else:
                #log_file.json을 data로 읽고
                with open(log_filename, 'r', encoding='utf8') as data:
                    json_data = json.load(data)
                    #url 방문 기록이 있다면 last_update_date에 기록
                    if json_data.get(input_url) != None:
                        print("have record")
                        update_date = json_data[input_url]
                    with open(log_filename, 'w') as data2:
                        json_data[input_url] = today
                        json.dump(json_data, data2, indent='\t')
            print(top_dir)
            #일단
            url_loop(top_dir, update_date)

    #크롤러 시작
    start_crawler(input_url, top_dir)


if __name__ == "__main__":
    url01 = "https://www.bymono.com/"
    url02 = "https://www.bignjoy.com/"
    url03 = "https://uuumall.kr/"
    url04 = "https://www.hiver.co.kr/"  # 존재하는 a tag를 제대로 파싱하지 못함 >> loading창에 a tag가 없어서 그런듯
    url05 = "https://monoforce.co.kr/product/detail.html?product_no=18960"  #javascript가 없으면 페이지가 거의 비어있음 + img에 a태그 없음
    urlXX = "https://www.gap.com/"  #javascript 많이 씀
    url0l = "https://linkda.cc/"  #여러 외부링크를 모아둔 사이트: 외부링크 차단 기능 테스트용 >> 통과함
    url07 = 'https://www.ssfshop.com/8Seconds/'
    url08 = 'https://www.ssfshop.com/8Seconds/main?brandShopNo=BDMA07A01&brndShopId=8SBSS'  #8세컨즈, 자바스크립트 필수
    #지그재그 계열
    url10 = 'http://www.beginning.kr/'
    url11 = 'https://www.benito.co.kr/'
    url12 = 'https://like-you.kr/'
    url13 = 'http://www.secretlabel.co.kr/'
    url14 = 'https://www.wonlog.co.kr/'
    url15 = 'http://www.zemmaworld.com/'
    #골프웨어
    url20 = 'https://www.nbkorea.com/etc/collection.action?collectionIdx=3424'
    url21 = 'https://www.luxgolf.net/'
    url22 = 'http://www.mariomall.co.kr/'
    url23 = 'https://www.rhone.com/'
    url24 = 'https://shop.lululemon.com/p/men-pants/Abc-Slim-5-Pocket-32/_/prod9390007?color=32476'
    url2X = 'https://info.lululemon.com/help'  
    url25 = 'https://www.underarmour.com/en-us/c/mens/sports/golf/'  #session 기반으로 돌렸을 때 Access Denied 걸림
    url26 = 'https://www.hugoboss.com/us/relaxed-fit-shirt-in-oxford-cotton-with-exclusive-logo/hbna50457438_492.html#wrapper'
    url27 = 'https://www.hugoboss.com/us/men-t-shirts/'
    #NON_JAVASCRIPT_LIST
    #해외몰 특선: mytheressa 같은 영미권 웹사이트 찾아보기
    url30 = 'https://www.net-a-porter.com/en-us/'
    url31 = 'https://www.mrporter.com/en-us/'  #밴
    url32 = 'https://www.bergdorfgoodman.com/'  #밴
    url00 = "https://www.mytheresa.com/"
    url33 = 'https://www.ssense.com/en-us/'
    url34 = 'https://www.revolve.co.kr/'

    top_dir = "revolve"
    crawler(top_dir, url33)
