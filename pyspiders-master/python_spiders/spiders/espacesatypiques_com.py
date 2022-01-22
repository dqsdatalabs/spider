# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json, math
import re
import dateparser


class MySpider(Spider):
    name = 'espacesatypiques_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Espacesatypiques_PySpider_france_fr'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.espaces-atypiques.com/locations/?pl=&type=13&pmin=&pmax=&smin=&smax=&s=&order=ddesc", "property_type": "house"},
            {"url": "https://www.espaces-atypiques.com/locations/?pl=&type=12&pmin=&pmax=&smin=&smax=&s=&order=ddesc", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse,
                             meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for follow_url in response.xpath("//div[@id='annonces']//div[@class='photo-container']/a/@href").extract():
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            
        
        pagination = response.xpath("//div[contains(@class,'pagination')]/a[contains(@class,'next')]/@href").extract_first()
        if pagination:
            yield Request(pagination, callback=self.parse, meta={"property_type": response.meta.get('property_type')})
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Espacesatypiques_PySpider_"+ self.country + "_" + self.locale)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        item_loader.add_value("external_link", response.url)
        
        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        attr = "".join(response.xpath("//div[@class='reference right font2']/span/text()").extract())
        item_loader.add_value("external_id", attr.split(".")[1].strip())

        room_cnt = "".join(response.xpath("normalize-space(//div[@class='info-content']/ul/li/text())").extract())
        if room_cnt:
            if "pièces" in room_cnt:
                room_cnt = room_cnt.split("pièces")[0].strip()
            else:
                room_cnt = room_cnt.split("pièce")[0].strip()
            item_loader.add_value("room_count",room_cnt)
  

        bathroom_count = response.xpath("//div[@id='infos-cles']/div[contains(.,'Salle de bain') or contains(.,'Salles de bain')]/div[contains(@class,'info-value')]/text()").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)

        rent = "".join(response.xpath("//div[@id='infos-resume']/span[4]").extract())
        if rent:
            price=rent.strip().replace(" ","")
            item_loader.add_value("rent_string",price)

        item_loader.add_xpath("landlord_name","normalize-space(//div[@class='infos-nego']/div[2]/text())")
        phone=response.xpath("normalize-space(//div[@class='click-tel orange']/a/text())").extract_first()
        item_loader.add_value("landlord_phone", phone)
        item_loader.add_xpath("landlord_email","normalize-space(//div[@class='email orange'])")

        desc="".join(response.xpath("//div[@id='annonce-content']/div/p/text()").extract())
        if desc:
            item_loader.add_value("description",desc)
            if "meublé" in desc.lower():
                item_loader.add_value("furnished",True)
            
        if "Disponible" in desc:
            available_date = desc.split("Disponible")[1].split("Lo")[0].replace(".","")
            available_d = ""
            if "du" in available_date:
                available_d = available_date.split("du")[1].strip()
            elif "le" in available_date:
                available_d = available_date.split("le")[1].strip()
            if available_d:
                date_parsed = dateparser.parse( 
                    available_d, date_formats=["%d/%m/%Y"]
                )
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
                    print(date2)
        
        square = response.xpath("//div[@id='infos-resume']/span[3]/text()[.!=' M2']").extract_first()
        if square:
            item_loader.add_value("square_meters", str(math.ceil(float(square.split("M2")[0]))))
        else:
            unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(m²|meters2|metres2|meter2|metre2|mt2|m2|M2)",desc.replace(",","."))
            if unit_pattern:
                sq=int(float(unit_pattern[0][0]))
                item_loader.add_value("square_meters", str(sq))

        
        zipcode=response.xpath("//div[@id='infos-resume']/span[2]/text()").extract_first()
        item_loader.add_value("zipcode", zipcode )
        city=response.xpath("//div[@id='infos-resume']/span[1]/text()").extract_first()
        item_loader.add_value("city",city )
        item_loader.add_value("address", city+" "+zipcode)

        latitude_longitude = response.xpath("//script[contains(.,'lat') and contains(.,'lon')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("lat =")[1].split(";")[0].strip()
            longitude = latitude_longitude.split("lon =")[1].split(";")[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        images=[x for x in response.xpath("//div[@id='gallery']//div/a/@href").getall()]
        if images:
            item_loader.add_value("images",images)
        
        label=response.xpath("//div[@id='dpe']/div/@class").extract_first()
        if label:
            item_loader.add_value("energy_label", label.split("-")[1])

        deposit=response.xpath("//div[@class='info-plus']/div/ul/li[contains(.,'Dépôt')]/text()").extract_first()
        if deposit:
            item_loader.add_value("deposit", deposit.split(":")[1].split("€")[0].replace(" ",""))

        charge=response.xpath("//div[@class='info-plus']/div/ul/li[contains(.,'charges')]/text()").extract_first()
        if charge:
            item_loader.add_value("utilities", charge.split(":")[1].split("€")[0])
            
        floor=response.xpath("//div[@class='info-plus']/div/ul/li[contains(.,'Étage')]/text()").extract_first()
        if floor:
            item_loader.add_value("floor", floor.split(":")[1].strip())

        yield item_loader.load_item()