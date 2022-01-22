# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from html.parser import HTMLParser
import re

class MySpider(Spider):
    name = 'swixim_fr_paris_republique'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Swiximparisrepublique_PySpider_france_fr'

    formdata = {
        "country": "FR",
        "nature": "2",
        "type":"",
        "range":"",
        "rooms":"",
        "bedrooms":"",
        "area_min":"", 
        "area_max":"", 
        "price_min":"",
        "price_max":"", 
        "reference":"", 
        "customroute":"",
        "search_page": "homepage",
    }
    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.swixim.fr/fr/search/"
            }
            
        ]
        for url in start_urls:
        
            yield FormRequest(url=url.get('url'),callback=self.parse,formdata=self.formdata)


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//li[@class='ad']/a"):
            dontallow=response.xpath("./ul/li[1]/text()").get()
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
            
        nextbutton=response.xpath("//li[@class='nextpage']/a/@href").get()
        if nextbutton:
            yield FormRequest(response.urljoin(nextbutton), callback=self.parse,formdata=self.formdata)
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)

        item_loader.add_value("property_type", response.meta.get('property_type')) 
        item_loader.add_value("external_link", response.url)
        
        title = " ".join(response.xpath("//article/div/h1/text()").extract())
        if title:
            item_loader.add_value("title", re.sub("\s{2,}", " ", title.strip()))
        dontallow =" ".join(response.xpath("//article/div/h1/text()").extract())
        if dontallow and ("terrain" in dontallow.lower() or "local" in dontallow.lower() or "parking" in dontallow.lower() or "garage" in dontallow.lower() or "commerce" in dontallow.lower() or "droit" in dontallow.lower() or "bureau" in dontallow.lower()):
            return

        property_type=item_loader.get_output_value("title")
        if property_type:
            item_loader.add_value("property_type", get_p_type_string(property_type)) 
        address = "".join(response.xpath("//article/div/h1/text()[2]").extract())
        if address:
            item_loader.add_value("address", address.strip())
            try:
                city = address.strip().split(" ")[0].strip()
                item_loader.add_value("city", city)
            except:
                pass

        room_count=response.xpath("//ul/li[contains(.,'chambres')]//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" "[0]))


        bathroom_count=response.xpath("//ul/li[contains(.,'Salle de bains') or contains(.,'Salle de douche') ]/text()").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0])
            
        square_mt=response.xpath("//article/div/ul/li[contains(.,'m²')]/text()").extract_first()
        if square_mt:
            item_loader.add_value("square_meters", square_mt.split(" ")[0].strip())
        
        
        rent=response.xpath("//article/div/ul/li[contains(.,'€')]//text()").extract_first()
        if rent:
            item_loader.add_value("rent", rent.split("+")[0].strip().replace(" ",""))
        item_loader.add_value("currency","EUR")
        
            
        latitude_longitude = response.xpath("//aside[@id='showMap']//script/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('L.marker([')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('L.marker([')[1].split(',')[1].split(']')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
            
        
        desc="".join(response.xpath("//p[@class='comment']/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        external_id=response.xpath("//p[@class='comment']//text()[contains(.,'Ref')]").get()
        if external_id:
            if ":" in external_id:
                external_id=" ".join(desc.split("Ref : ")[-1].split(" ")[0]) 
                item_loader.add_value("external_id", external_id)
            elif "Ref:" in external_id:
                external_id=" ".join(desc.split("Ref:")[1].split(" ")[0]) 
                item_loader.add_value("external_id", external_id)
            elif "-" in external_id:
                external_id=" ".join(desc.split("Ref")[1].split("-")[0]) 
                item_loader.add_value("external_id", external_id)
            else:
                external_id=response.xpath("//p[@class='comment']/text()[contains(.,'Réf ')]").get()
                if external_id:
                    external_id=" ".join(desc.split("Réf")[1].split(" ")[1:2]) 
                    item_loader.add_value("external_id", external_id)
                
        zipcode=response.xpath("//p[@class='comment']/text()").get()
        if zipcode:
            zipcode=" ".join(desc.split("(")[1:]) 
            item_loader.add_value("zipcode", zipcode.split(")")[:1])
        
        charges=response.xpath("//div[@class='legal details']/ul/li[contains(.,'Charges')]/span/text()").extract_first()
        if charges:
            item_loader.add_value("utilities", charges.split("€")[0].strip())
        
        deposit=response.xpath("//div[@class='legal details']/ul/li[contains(.,'Garantie')]/span/text()").extract_first()
        if deposit:
            item_loader.add_value("deposit", deposit.split("€")[0].replace(" ",""))
        
        images=[x for x in response.xpath("//div[@class='item resizePicture']/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            # item_loader.add_value("external_images_count", str(len(images)))
        
        name=response.xpath("//div[@class='userBlock']/p/strong/text()").extract_first()
        if name:
            item_loader.add_value("landlord_name", name)
        
        phone=response.xpath("//div[@class='userBlock']/p/span/text()").extract_first()
        if phone:
            item_loader.add_value("landlord_phone", phone)
        
        email=response.xpath("//div[@class='userBlock']/p/span/a/text()").extract_first()
        if email:
            item_loader.add_value("landlord_email", email) 
    
        
        yield item_loader.load_item()
    

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("appartement" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "maison" in p_type_string.lower()):
        return "house"
    else:
        return "house"