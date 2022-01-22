# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from  geopy.geocoders import Nominatim
import re

class MySpider(Spider):
    name = 'guy_hoquet_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Guyhoquet_PySpider_france_fr"
    headers = {
        'content-type': "application/json; charset=utf-8",
        'accept': "*/*",
        'accept-encoding': "gzip, deflate, br",
        'accept-language': "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        'cookie': "_gcl_au=1.1.1337956913.1603524819; _ga=GA1.2.1545143180.1603524819; _gid=GA1.2.493799296.1603524819; cookiesPreferences=%5B%5D; laravel_session=gra-varnish~qF1I5K3hjPBdKn5BlWmuDBMCvdzfYwAtVjOZeGL2; XSRF-TOKEN=eyJpdiI6InRhV1VPb2l5VHFhYWRBMTVcL3M2cGZ3PT0iLCJ2YWx1ZSI6ImxpRmlXYzcrdnBVQjBmaGxSXC9BZUowZlJjRGpXTEw4UUtKMWE4MjdLbVJidXBYUXpZeWg4ZGxheUZrUmZnU3haMlwvOWR2WG9uTU82S1lFeCs5YXJ6SWc9PSIsIm1hYyI6IjAzZmVmZTkwOTRhNTgyMzIwYmU3YWM0NjY1YWQ0MGNkM2E3MTk0NWFmZWU3MmMzYTk4NDk2YjE1ZDQzYTc3MTYifQ%3D%3D",
        'referer': "https://auterive.guy-hoquet.com/biens/result",
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
           
            {"url": "https://auterive.guy-hoquet.com/biens/result?1&templates%5B%5D=properties&p=1&t=&filters%5B10%5D%5B%5D=2&filters%5B30%5D%5B%5D=appartement&with_markers=false&_=1603700724670", "property_type": "apartment"},
	        {"url": "https://auterive.guy-hoquet.com/biens/result?1&templates%5B%5D=properties&p=1&t=&filters%5B10%5D%5B%5D=2&filters%5B30%5D%5B%5D=maison&with_markers=false&_=1603700766119", "property_type": "house"},
        ] 
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
        data_url=sel.xpath("//div[contains(@class,'resultat-item')]/a/@href").extract()
        for item in data_url:
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item ,dont_filter=True, meta={'property_type': response.meta.get('property_type')})
            seen = True
        if response.meta.get('property_type')=="apartment":
            if page == 2 or seen:
                url = f"https://auterive.guy-hoquet.com/biens/result?1&templates%5B%5D=properties&p={page}&t=&filters%5B10%5D%5B%5D=2&filters%5B30%5D%5B%5D=appartement&with_markers=false&_=1603700724670"
                yield Request(url, callback=self.parse, headers=self.headers, dont_filter=True, meta={"page": page+1 ,'property_type': response.meta.get('property_type')})
           
        if response.meta.get('property_type')=="house":
            if page == 2 or seen:
                url = f"https://auterive.guy-hoquet.com/biens/result?1&templates%5B%5D=properties&p={page}&t=&filters%5B10%5D%5B%5D=2&filters%5B30%5D%5B%5D=maison&with_markers=false&_=1603700766119"
                yield Request(url, callback=self.parse, headers=self.headers, dont_filter=True, meta={"page": page+1 ,'property_type': response.meta.get('property_type')})
           
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        desc="".join(response.xpath("//span[@class='description-more']/text()").getall())
        if desc:
            if "pascale rocheron" in desc.lower():
                return
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc.replace("Guy Hoquet",""))
        
        
        item_loader.add_value("external_source", self.external_source)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        title = response.xpath("//div/h1/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title.replace("Guy Hoquet",""))
        
        
        rent="".join(response.xpath("//div[@class='price']/text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent)

        deposit="".join(response.xpath("//div[div[.='Dépôt de garantie']]/div[2]/text()").getall())
        if deposit:
            item_loader.add_value("deposit", deposit.strip())

        utilities="".join(response.xpath("//div[div[.='Provision sur charges']]/div[2]/text()").getall())
        if utilities:
            item_loader.add_value("utilities", utilities.strip())

        floor="".join(response.xpath("//div[div[.='Nombre étage']]/div[2]/text()").getall())
        if floor:
            item_loader.add_value("floor", floor.strip())
        
        square_meters="".join(response.xpath(
            "//div[@class='biens-list']//div/i[contains(@class,'ico scale')]/parent::div//text()").getall())
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m²')[0].strip())
        
        room_count="".join(response.xpath(
            "//div[@class='biens-list']//div/i[contains(@class,'ico room')]/parent::div//text()").getall())
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(' ')[0])
        
        latitude_longitude = response.xpath("//script[@type='text/javascript'][contains(.,'Lat')]/text()").get()
        if latitude_longitude:
                latitude = latitude_longitude.split(" Lat = '")[1].split("';")[0]
                longitude = latitude_longitude.split("Lng = '")[1].split("';")[0]
   
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)

        address=response.xpath("//div[@class='add']/text()").get()
        if address:
            item_loader.add_value("address",address)
            item_loader.add_value("city",address.split(" ")[0])
            item_loader.add_value("zipcode",address.split(" ")[1])
           

        bathroom=response.xpath("normalize-space(//div[@class='biens-item']/div/i[@class='ico bath']/following-sibling::div/text())").get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom.split(" ")[0].strip())
        
        external_id=response.xpath("//div[@class='code']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())

        
        if desc:
            item_loader.add_value("description", desc.strip())

        label="".join(response.xpath("//div[@class='dpe']/img/@data-src[contains(.,'dpe')]").getall())
        if label:
            item_loader.add_value("energy_label", label.strip().split("dpe_")[1].split(".")[0])
        
        floor=response.xpath("//div[@class='horaires-item']/div/div[contains(.,'Etage')]//following-sibling::div/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
            
        images=[x for x in response.xpath("//div[@class='de-biens-slider']/div/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
             
        landlord_name="".join(response.xpath("//div[contains(@class,'contact-agence')]/div[@class='name']//text()").getall())
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())
        item_loader.add_xpath("landlord_phone", "//div[contains(@class,'contact-agence')]//a[@itemprop='telephone']//text()")
        yield item_loader.load_item()