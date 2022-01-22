# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
from html.parser import HTMLParser
import math

class MySpider(Spider):
    name = 'i2cimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    custom_settings = {      
        "PROXY_ON": True,
    }
    
    headers = {
        'content-type': "multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW",
        'cache-control': "no-cache",
    }
    def start_requests(self):
        start_urls = [
            {
                "type" : 1,
                "property_type" : "apartment"
            },
            {
                "type" : 2,
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            type_bien = url.get("type")
            payload = f"------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"location_search[typeBien][]\"\r\n\r\n{type_bien}\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"location_search[loyer_max]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"location_search[noMandat]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"location_search[tri]\"\r\n\r\nloyerCcTtcMensuel|asc\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"location_search[referenceInterne]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"location_search[secteurByFirstLetterMandat]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"location_search[loyer_min]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"location_search[piece_min]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"location_search[piece_max]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW--"

            yield Request("https://www.i2cimmobilier.com/fr/locations",
                                 callback=self.parse,
                                 body=payload,
                                 method="POST",
                                 headers=self.headers,
                                 meta={'property_type': url.get('property_type'), "type": type_bien})


    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        
        seen = False
        for item in response.xpath("//a[@class='img_bien']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True


        if page == 2 or seen:
            url = f"https://www.i2cimmobilier.com/fr/locations/{page}"
            type_bien = response.meta.get("type")

            payload = f"------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"location_search[typeBien][]\"\r\n\r\n{type_bien}\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"location_search[loyer_max]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"location_search[noMandat]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"location_search[tri]\"\r\n\r\nloyerCcTtcMensuel|asc\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"location_search[referenceInterne]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"location_search[secteurByFirstLetterMandat]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"location_search[loyer_min]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"location_search[piece_min]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"location_search[piece_max]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW--"

            yield Request(url,
                        callback=self.parse,
                        body=payload,
                        method="POST",
                        headers=self.headers,
                        meta={'property_type': response.meta.get('property_type'), "type": type_bien, "page":page+1})


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        desc = "".join(response.xpath("//div[@id='description']//text()").extract())
        if desc :
            item_loader.add_value("description", desc.strip())

        title = re.sub(r'\s{2,}', ' ', ("".join(response.xpath("//div[@class='ref']/../div[contains(@class,'prix')]//text()").getall()).replace("\n",""))).strip()
        if title :
            item_loader.add_value("title", title)
            room_count = response.xpath("//span[contains(.,'Chambre')]/following-sibling::b/text()").get()
            if room_count:
                item_loader.add_value("room_count",room_count)
            elif "pièce" in title:
                    room=title.split("pièce")[0].split(" ")[1]
                    item_loader.add_value("room_count",room)
            elif 'bedroom' in desc:
                room_count = re.search(r'(\d)\sbedroom', desc)
                if room_count:
                    item_loader.add_value("room_count",room_count.group(1))   

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "I2cimmobilier_PySpider_"+ self.country + "_" + self.locale)
        
        item_loader.add_value("external_link", response.url)
        ext_id=response.xpath("substring-after(//div[@class='container']//span[@class='reference']//text(),':')").extract_first()
        if ext_id:
            item_loader.add_value("external_id", ext_id.strip())
        city=response.xpath("//div[@class='left prix']/span[@class='commune']//text()").extract_first()
        if city:
            item_loader.add_value("city", city)
            item_loader.add_value("address",city)
        
      
        price = re.sub('\s{2,}', ' ', ("".join(response.xpath("//span[@class='prix has_sup'][contains(.,'Prix')]//text()").getall()).replace("\n",""))).strip()
        if price :           
            if "/" in price:
                price=price.split("/")[0]
                if "Prix :" in price:
                    price=price.split("Prix :")[1]
                    item_loader.add_value("rent_string", price.strip().replace(" ",""))
        

        square_meters = response.xpath(
            "//div[contains(@class,'contenuCriteres')]//p/span[contains(.,'habitable')]/following-sibling::b/text()"
            ).extract_first()
        if square_meters :
            square_meters = math.ceil(float(square_meters.replace("m²","").strip()))
            item_loader.add_value("square_meters", str(square_meters))
        
        
        zipcode=response.xpath("//div[@class='left prix']/span[@class='cp']//text()").extract_first()
        if zipcode:
            zipcode=zipcode.strip("(").strip(")")
            item_loader.add_value("zipcode", zipcode.strip())
        
        floor = response.xpath("//div[@class='criteres row']/div[contains (.,'Étage')]//b/text()").extract_first()
        if floor :  
           item_loader.add_value("floor", floor)
        
        if "piscine" in desc:
            item_loader.add_value("swimming_pool", True)
        
        count = desc.count("\u00e9tage")
        if "\u00e9tage" in desc and count ==1:
            floor = desc.split("\u00e9tage")[0].strip().split(" ")[-1]
        elif "\u00e9tage" in desc and count ==2:
            floor = desc.split("\u00e9tage")[1].strip().split(" ")[-1]
        
        if floor and "dernier" in floor:
            item_loader.add_value("floor"," top")
        elif floor:
            floor = floor.replace("ème","").replace("ième","").replace("ier","").replace("er","").replace("e","")
            if floor.isdigit():
                item_loader.add_value("floor", floor)
            
        deposit ="".join(response.xpath("//div[@class='charges']//div[contains(.,'Dépôt de garantie')]//text()").extract())
        if deposit :            
            dp=deposit.split(":")[1].split("€")[0].strip()
            item_loader.add_value("deposit", dp.replace(" ",""))

        utilities ="".join(response.xpath("//div[@class='charges']//div[contains(.,'charge')]//text()").extract())
        if utilities :            
            utility=utilities.split(":")[1].split("€")[0].strip()
            item_loader.add_value("utilities", utility.replace(" ",""))
        
        energy_label=response.xpath("//div[@class='bilan_conso']/div/@class").extract_first()
        if energy_label :
            item_loader.add_value("energy_label", energy_label.split("dpe_")[1])

        terrace = response.xpath("//div[@class='criteres row']/div[contains (.,'Terrasse ')]//b/text()").extract_first()
        if terrace :
            item_loader.add_value("terrace", True)

        img=response.xpath("//div[@id='carousel-photo']//a[@class='item photo']//img/@src").extract() 
        if img:
            images=[]
            for x in img:
                images.append(x)
            if images:
                item_loader.add_value("images",  list(set(images)))
         
        furnished = response.xpath("//div[@class='criteres row']/div[contains (.,'Meublé')]//b/text()").extract_first()
        if furnished :
            if furnished == "Non":
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        
        bathroom_count = response.xpath(
            "//div[contains(@class,'contenuCriteres')]//p/span[contains(.,'Salle de')]/following-sibling::b/text()"
            ).get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        
        item_loader.add_value("landlord_name", "I2C Immobilier")
        item_loader.add_value("landlord_phone", "04 50 40 40 20")
 
        iframe = response.xpath("//div[contains(@class,'bien_map')]/iframe/@src").extract_first() 
        if iframe:     
            yield Request(url=iframe,callback=self.parse_latlong,meta={"item_loader":item_loader})
        else:
            yield item_loader.load_item()

    def parse_latlong(self, response):

        item_loader = response.meta.get('item_loader')

        lat_long = "".join(response.xpath("//script[@id='csAppState']//text()").extract())
        if lat_long:
            lat = lat_long.split('"geoloc": {')[1].split('"lat": "')[1].split('"')[0]
            lon = lat_long.split('"geoloc": {')[1].split('"lon": "')[1].split('"')[0]
            item_loader.add_value("latitude",lat)
            item_loader.add_value("longitude",lon)
     
        yield item_loader.load_item()

        
     