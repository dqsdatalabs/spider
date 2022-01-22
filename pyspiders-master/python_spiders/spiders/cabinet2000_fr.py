# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
import re

class MySpider(Spider):
    name = 'cabinet2000_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = 'Cabinet2000_PySpider_france_fr'
    custom_settings = { 
         
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,

    }   
    def start_requests(self):

        start_urls = [
            {
                "type" : 18,
                "property_type" : "apartment"
            },
            {
                "type" : 2,
                "property_type" : "apartment"
            },
            
        ] #LEVEL-1

        for url in start_urls:
            r_type = str(url.get("type"))
            payload = {
                "data[Search][offredem]": "2",
                "data[Search][idtype]": r_type,
                "data[Search][idvillecode]": "void",
                "data[Search][NO_DOSSIER]": "",
                "data[Search][prixmin]": "",
                "data[Search][prixmax]": "",
                "data[Search][surfmin]": "",
                "data[Search][surfmax]": "",
                "data[Search][piecesmin]": "",
                "data[Search][piecesmax]": "",
            }

            yield FormRequest(url="http://www.cabinet2000.fr/recherche/",
                                callback=self.parse,
                                formdata=payload,
                                dont_filter=True,
                                #headers=self.headers,
                                meta={'property_type': url.get('property_type')})
            
    # 1. FOLLOWING
    def parse(self, response): 
        for item in response.xpath("//h1[@itemprop='name']/a/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        title="".join(response.xpath("//div[contains(@class,'bienTitle')]/h1/text()").getall())
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title))
        
        rent="".join(response.xpath("//div[@id='infosfi']/p[contains(.,'Loyer')]/span[contains(@class,'valueInfos')]/text()").getall())
        if rent:
            item_loader.add_value("rent", rent.replace(" ","").split(",")[0].split("€")[0].replace("\xa0",""))
        item_loader.add_value("currency", "EUR")
        
        square_meters=response.xpath("//div[@id='infos']/p[contains(.,'habitable')]/span[contains(@class,'valueInfos')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(",")[0].strip())
        
        room_count=response.xpath("//div[@id='infos']/p[contains(.,'pièce')]/span[contains(@class,'valueInfos')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())

        city = response.xpath("//span[contains(.,'Ville')]/following-sibling::span/text()").get()
        zipcode = response.xpath("//span[contains(.,'Code postal')]/following-sibling::span/text()").get()
        if city and zipcode:
            item_loader.add_value("address", city.strip() + ' (' + zipcode.strip() + ')')
            item_loader.add_value("zipcode", zipcode.strip())
            item_loader.add_value("city", city.strip())
        
        latitude_longitude = response.xpath("//script[contains(.,'lng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('center: { lat :')[1].split(",")[0].strip()
            longitude = latitude_longitude.split('center: { lat :')[1].split(", lng:")[1].split('}')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        bathroom_count = response.xpath("//span[contains(.,'salle d')]/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        furnished=response.xpath("//div[@id='infos']/p[contains(.,'Meublé')]/span[contains(@class,'valueInfos')]/text()[not(contains(.,'NON'))]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        elevator=response.xpath("//div[@id='infos']/p[contains(.,'Ascenseur')]/span[contains(@class,'valueInfos')]/text()[not(contains(.,'NON'))]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        balcony=response.xpath("//div[@id='details']/p[contains(.,'Balcon')]/span[contains(@class,'valueInfos')]/text()[not(contains(.,'NON'))]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace=response.xpath("//div[@id='details']/p[contains(.,'Terrasse')]/span[contains(@class,'valueInfos')]/text()[not(contains(.,'NON'))]").get()
        if terrace:
            item_loader.add_value("terrace", True)
            
        parking=response.xpath("//div[@id='details']/p[contains(.,'parking')]/span[contains(@class,'valueInfos')]/text()[not(contains(.,'NON'))]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        deposit="".join(response.xpath("//div[@id='infosfi']/p[contains(.,'garantie')]/span[contains(@class,'valueInfos')]/text()").getall())
        if deposit:
            item_loader.add_value("deposit", int(float(deposit.split("€")[0].strip().replace(',', '.').replace(" ",""))))
        
        utilities="".join(response.xpath("//div[@id='infosfi']/p[contains(.,'Charge')]/span[contains(@class,'valueInfos')]/text()").getall())
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0].strip())
        
        desc=response.xpath("//article/p[@itemprop='description']//text()").get()
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc))
        
        images=[x for x in response.xpath("//div[@class='mainImg']/ul/li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        external_id=response.xpath("//div[@class='themTitle']/h1/span[contains(@class,'ref')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("Ref")[1].strip())
        
        item_loader.add_value("landlord_name", "Cabinet2000")
        item_loader.add_value("landlord_phone", "01 39 91 73 54")
        item_loader.add_value("landlord_email", "cabinet2000domont@sfr.fr")
        
        yield item_loader.load_item()
