# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'guyhoquetimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'

    headers = {
        'content-type': "application/json; charset=utf-8",
        'accept': "*/*",
        'accept-encoding': "gzip, deflate, br",
        'accept-language': "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        'cookie': "_gcl_au=1.1.1337956913.1603524819; _ga=GA1.2.1545143180.1603524819; _gid=GA1.2.493799296.1603524819; laravel_session=gra-varnish~qF1I5K3hjPBdKn5BlWmuDBMCvdzfYwAtVjOZeGL2; _gat_UA-18452241-12=1; _gat_gtag_UA_18452741_1=1; XSRF-TOKEN=eyJpdiI6Iml0Znk3eTYya1o1VmxESXBhcWRIWXc9PSIsInZhbHVlIjoiZUZqZTdjYnBYc0RMUW1xMWdQQXdXNU93ZWNlVnBxOGx6ZWhXajQzOGV0QUNqNUcxTEFjeDUrRTRleVl4XC9QcFhCbkVDUm5tTVk1NFNDMCtZXC92bHEwdz09IiwibWFjIjoiYzQwYmJhMDMzYWEyMTQ3ZTdlZTQ1NDgxZjExOWVmNDBiZWIwZjlkNGZiNDk5MGY3Y2IwYTUwMTZiNjNhMTk0YSJ9",
        'referer': "https://montrouge.guy-hoquet.com/biens/result",
        'sec-fetch-dest': "empty",
        'sec-fetch-mode': "cors",
        'sec-fetch-site': "same-origin",
        'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36",
        'x-csrf-token': "QfvBKOnmVzLq1waPGNQ3bFo5m83q4cE2xlk2kTYn",
        'x-requested-with': "XMLHttpRequest",
        'cache-control': "no-cache",
    }
    
    def start_requests(self):
        start_urls = [
            {"url": "https://montrouge.guy-hoquet.com/biens/result?1&templates%5B%5D=properties&p=1&t=&filters%5B10%5D%5B%5D=2&filters%5B30%5D%5B%5D=appartement&with_markers=false&_=1603704537599", "property_type": "apartment"},
	        {"url": "https://montrouge.guy-hoquet.com/biens/result?1&templates%5B%5D=properties&p=1&t=&filters%5B10%5D%5B%5D=2&filters%5B30%5D%5B%5D=maison&with_markers=false&_=1603704669971", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            headers=self.headers,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        data=json.loads(response.body)
        data_html = data["templates"]["properties"]
        sel = Selector(text=data_html, type="html")
        
        page = response.meta.get('page', 2)
        seen = False
        data_url=sel.xpath("//a[contains(@class,'property_link')]/@href").extract()
        
        for item in data_url:
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item ,dont_filter=True, meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        if response.meta.get('property_type')=="apartment":
            if page == 2 or seen:
                url = f"https://montrouge.guy-hoquet.com/biens/result?1&templates%5B%5D=properties&p={page}&t=&filters%5B10%5D%5B%5D=2&filters%5B30%5D%5B%5D=appartement&with_markers=false&_=1603704537599"
                yield Request(url, callback=self.parse, headers=self.headers, dont_filter=True, meta={"page": page+1 ,'property_type': response.meta.get('property_type')})
        
        if response.meta.get('property_type')=="house":
            if page == 2 or seen:
                url = f"https://montrouge.guy-hoquet.com/biens/result?1&templates%5B%5D=properties&p={page}&t=&filters%5B10%5D%5B%5D=2&filters%5B30%5D%5B%5D=maison&with_markers=false&_=1603704669971"
                yield Request(url, callback=self.parse, headers=self.headers, dont_filter=True, meta={"page": page+1 ,'property_type': response.meta.get('property_type')})
     

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Guyhoquetimmobilier_PySpider_"+ self.country + "_" + self.locale)

        desc="".join(response.xpath("//span[@class='description-more']/text()").getall())
        if "pascale rocheron" in desc.lower():
            return

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_xpath("title", "//div[@class='container']/h1/text()")
        item_loader.add_value("external_link", response.url)
        
        rent="".join(response.xpath("//div[@class='price']/text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))
        
        
        square_meters=response.xpath("//div[@class='biens-list']//div/i[contains(@class,'ico scale')]/following-sibling::div//text()").get()
        if square_meters:
            square_meters = int(float(square_meters.split('m²')[0].strip()))
            item_loader.add_value("square_meters",square_meters )
        
        
        room_count="".join(response.xpath(
            "//div[@class='biens-list']//div/i[contains(@class,'ico room')]/parent::div//text()").getall())
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(' ')[0])
        
        bathroom_count="".join(response.xpath(
            "//div[@class='biens-list']//div/i[contains(@class,'bath')]/parent::div//text()").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip().split(' ')[0])
        else:
            bathroom=''.join(response.xpath("//div[contains(@class,'horaires-ttl')][contains(.,'Salle(s) d')]/following-sibling::div/text()").getall())
            if bathroom:
                item_loader.add_value("bathroom_count", bathroom.strip())

        
        latitude_longitude = response.xpath("//script[@type='text/javascript'][contains(.,'Lat')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split(" Lat = '")[1].split("';")[0]
            longitude = latitude_longitude.split("Lng = '")[1].split("';")[0]
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        address=response.xpath("//div[@class='add']/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(" ")[0])
            item_loader.add_value("zipcode", address.split(" ")[1])
        
        furnished = response.xpath("//div[contains(@class,'horaires-ttl')][contains(.,'Meubl')]/following-sibling::div/text()").get()
        if furnished and "Non" in furnished:
            item_loader.add_value("furnished", False)
        if furnished and "Oui" in furnished:
            item_loader.add_value("furnished", True)
            
        external_id=response.xpath("//div[@class='code']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())

        desc="".join(response.xpath("//span[@class='description-more']/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        floor=response.xpath("//div[@class='horaires-item']/div/div[contains(.,'Etage')]//following-sibling::div/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
            
        images=[x for x in response.xpath("//div[@class='de-biens-slider']/div/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        utilties=response.xpath(
            "//div[@class='horaires-item']/div/div[contains(.,'charge')]//following-sibling::div/text()").get()
        if utilties:
            item_loader.add_value("utilities", utilties.split('€')[0].strip())

        if response.xpath("//i[@class='ico parking']").get(): item_loader.add_value("parking", True)
        
        deposit=response.xpath(
            "//div[@class='horaires-item']/div/div[contains(.,'garantie')]//following-sibling::div/text()").get()
        if deposit:
            item_loader.add_value("deposit", int(float(deposit.split('€')[0].strip())))

        item_loader.add_value("landlord_name", "GUY HOQUET MONTROUGE")
        item_loader.add_value("landlord_phone", "01 47 35 31 31")
        
        yield item_loader.load_item()