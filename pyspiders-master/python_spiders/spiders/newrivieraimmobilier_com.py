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
from html.parser import HTMLParser
import re

class MySpider(Spider):
    name = 'newrivieraimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    custom_settings = { 
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,
    }

    headers={
        "Cookie": "cookies_hasVerified=true; cookies_googleAnalytics=true; PHPSESSID=qp1imkvr6epbd0399j6nikr25d; SRV=c42; _ga=GA1.2.1549697909.1638794390; _gid=GA1.2.776967008.1638794390; _fbp=fb.1.1638794389913.486598799; _gat_UA-127077007-1=1",
        "Referer": "https://www.newrivieraimmobilier.com/recherche/",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36"
    }

    def start_requests(self):
        start_urls = [
            {
                "type" : "2_4",
            },
            {
                "type" : "1",
            },
            

        ] #LEVEL-1

        for url in start_urls:
            r_type = url.get("type")
            formdata={
                "data[Search][offredem]": "2",
                "data[Search][idtype][]":str(r_type),
                "data[Search][prixmax]": "",
                "data[Search][piecesmin]": "",
                "data[Search][NO_DOSSIER]": "",
                "data[Search][distance_idvillecode]": "",
                "data[Search][prixmin]": "",
                "data[Search][surfmin]": "",
            }
            yield FormRequest("https://www.newrivieraimmobilier.com/recherche/",
                                    callback=self.parse,
                                    formdata=formdata,headers=self.headers)


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//ul[@class='listingUL']/li/@onclick").extract():
            follow_url = response.urljoin(item.split("=")[1].replace("\'","").strip())
            yield Request(follow_url, callback=self.populate_item)
            

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Newrivieraimmobilier_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        rent="".join(response.xpath("//div[@class='prix-dt2']/text()").extract())
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))
        
        
        address=response.xpath("//div[@class='container']/ol/li[2]//text()").extract_first()
        if address:
            item_loader.add_value("address", address)

        city = response.xpath("//span[contains(.,'Ville')]/following-sibling::span/text()").get()
        if city:
            item_loader.add_value("city", city.strip())

        bathroom_count = response.xpath("//span[contains(.,'salle de bains') or contains(.,\"salle d'eau\")]/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        
        square_meters=response.xpath("//div[@id='infos']/p/span[contains(.,'Surface')]//following-sibling::span/text()").extract_first()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.strip())
        
            
        room_count=response.xpath("//div[@id='infos']/p/span[contains(.,'pièces')]//following-sibling::span/text()").extract_first()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())       
        
        
        zipcode=response.xpath("//div[@id='infos']/p/span[contains(.,'Code')]//following-sibling::span/text()").extract_first()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        
        title=response.xpath("//div[@class='bienTitle']/h2/text()").extract_first()
        if title:
            item_loader.add_value("title", re.sub("\s{2,}", " ", title.strip().replace("\n","")))
        
        external_id=response.xpath("//div[@class='bienTitle']/span/text()").extract_first()
        if external_id:
            item_loader.add_value("external_id", external_id.split(" ")[1])
        
        furnished=response.xpath("//div[@id='infos']/p/span[contains(.,'Meublé')]//following-sibling::span/text()").extract_first()
        if furnished!='Non renseigné':
            item_loader.add_value("furnished", True)  
        
        terrace=response.xpath("//div[@id='details']/p/span[contains(.,'Terrasse')]//following-sibling::span/text()").extract_first()
        if terrace=='OUI':
            item_loader.add_value("terrace", True)
        
        swimming_pool=response.xpath("//div[@id='details']/p/span[contains(.,'Terrain piscinable')]//following-sibling::span/text()").extract_first()
        if swimming_pool=='OUI':
            item_loader.add_value("swimming_pool", True)
        
        deposit=response.xpath("//div[@id='infosfi']/p/span[contains(.,'garantie')]//following-sibling::span/text()").extract_first()
        if deposit:
            item_loader.add_value("deposit", deposit.split("€")[0].strip())
        
        utilities=response.xpath("//div[@id='infosfi']/p/span[contains(.,'Charges')]//following-sibling::span/text()").extract_first()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0].strip())
        
        desc="".join(response.xpath("//p[@itemprop='description']/text()").extract())
        if desc:
            item_loader.add_value("description", desc)
        
        images=[x for x in response.xpath("//ul[contains(@class,'imageGallery imageHC  loading')]/li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        latitude_longitude=response.xpath("//script[contains(.,'optionsCircle')]/text()").get()
        if latitude_longitude:
            lat=latitude_longitude.split('lat:')[1].split(',')[0].strip()
            lng=latitude_longitude.split('lng:')[1].split('}')[0].strip()
            if lat or lng:
                item_loader.add_value("latitude",lat)
                item_loader.add_value("longitude", lng)
        
        item_loader.add_value("landlord_name", "NR IMMOBILIER")
        item_loader.add_value("landlord_phone", "06 30 79 79 90")
        item_loader.add_value("landlord_email", "contact@nrimmobilier.com")
        
        yield item_loader.load_item()