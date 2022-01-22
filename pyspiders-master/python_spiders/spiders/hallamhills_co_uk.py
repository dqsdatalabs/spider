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
import dateparser
from word2number import w2n

class MySpider(Spider):
    name = 'hallamhills_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    start_urls = ["http://www.hallamhills.co.uk/?id=23894&action=view&route=search&view=list&input=S10&jengo_property_for=2&jengo_category=1&jengo_radius=100&jengo_property_type=-1&jengo_order=6&jengo_min_price=0&jengo_max_price=99999999999&jengo_min_beds=0&jengo_max_beds=9999&pfor_complete=&pfor_offer=&trueSearch=&searchType=postcode&latitude=&longitude=#total-results-wrapper"]

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        max_page = response.xpath("//div[@class='rnav-prev']/../b[2]/text()").get().strip()

        seen = False
        for i in response.xpath("//section/div"):
            f_url = response.urljoin(i.xpath(".//a[contains(.,'View Details')]/@href").get())
            room_count = i.xpath("//li/a/i[contains(@class,'bed')]/parent::a/text()").get()
            bathroom_count = i.xpath("//li/a/i[contains(@class,'bath')]/parent::a/text()").get()
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={'room_count': room_count, 'bathroom_count': bathroom_count}
            )
            seen = True
        
        if page <= int(max_page):
            f_url = f'http://www.hallamhills.co.uk/?id=23894&action=view&route=search&view=list&input=S10&jengo_radius=100&jengo_property_for=2&jengo_category=1&jengo_property_type=-1&jengo_min_price=0&jengo_max_price=99999999999&jengo_min_beds=0&jengo_max_beds=9999&jengo_min_bathrooms=&jengo_max_bathrooms=9999&min_land=&max_land=&min_space=&max_space=&jengo_branch=&country=&daterange=&jengo_order=&trueSearch=&searchType=postcode&latitude=&longitude=&pfor_complete=on&pfor_offer=on&page={page}'
            yield Request(
                url=f_url,
                callback=self.parse,
                meta={"page" : page+1}
            )
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)

        is_rented = response.xpath("//span[@class='details-type']/text()[contains(.,'To let') or contains(.,'Let agreed')]").get()
        if is_rented: return

        prop_type = "".join(response.xpath("//span[@class='details-type']/text()").extract())
        if prop_type and ("flat" in prop_type.lower() or "apartment" in prop_type.lower()):
            item_loader.add_value("property_type", "apartment")
        elif prop_type and "house" in prop_type.lower():
            item_loader.add_value("property_type", "house")
        else:
            return
        item_loader.add_value("external_source", "Hallamhills_PySpider_"+ self.country + "_" + self.locale)
        
        title = response.xpath("//title/text()").get()
        item_loader.add_value("title", title)
        
        rent = response.xpath("//h2/text()").get()
        if "." in rent:
            item_loader.add_value("rent", rent.replace("£","").split(".")[0].replace(",",""))
        elif rent:
            item_loader.add_value("rent", rent.replace("£","").replace(",",""))
        
        item_loader.add_value("currency", "GBP")
        if "Studio" in response.meta.get("room_count") or "studio" in title.lower():
            item_loader.add_value("room_count", "1")
        elif "Bed" in title:
            room_count = title.split(" ")[0]
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_value("room_count", response.meta.get("room_count"))
            
        item_loader.add_value("bathroom_count", response.meta.get("bathroom_count"))
        
        address = response.xpath("//h1[contains(@class,'address1')]//text()").get()
        city = response.xpath("//h3[contains(@class,'areas')]/text()").get()
        if address or city:
            zipcode = city.split("|")[1].strip()
            item_loader.add_value("address", address.strip()+city.strip())
            item_loader.add_value("city", city.split("|")[0].strip())
            item_loader.add_value("zipcode", zipcode)
        
        lat_lng = response.xpath("//script[contains(.,'prop_lng')]/text()").get()
        if lat_lng:
            lat = lat_lng.split("prop_lat =")[1].split(";")[0].strip()
            lng = lat_lng.split("prop_lng =")[1].split(";")[0].strip()
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)
            
        desc = " ".join(response.xpath("//div/p[@class='dt-description']/parent::div//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc)
            item_loader.add_value("description", desc)
        
        available_date = response.xpath(
            "//div/p[@class='dt-description']/parent::div//text()[contains(.,'Available') or contains(.,'AVAILABLE')]"
            ).get()
        if available_date:
            available_date = available_date.lower().replace("available","").replace("*","").replace("!","")
            if "for" in available_date:
                available_date = available_date.split(" ")[1]
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            date_parsed2 = dateparser.parse(available_date, date_formats=["%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
            elif date_parsed2:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        if "floor" in desc.lower():
            floor = desc.lower().split("floor")[0].strip().split(" ")[-1]
            if "first" in floor or "second" in floor or "ground" in floor:
                item_loader.add_value("floor", floor)
            else:
                try:
                    floor = w2n.word_to_num(floor) 
                except:
                    floor = False
                if floor :
                    item_loader.add_value("floor", str(floor))
        
        if "balcony" in desc.lower():
            item_loader.add_value("balcony", True)
        
        if "dishwasher" in desc.lower():
            item_loader.add_value("dishwasher", True)    
        
        if "EPC RATING" in desc:
            item_loader.add_value("energy_label", desc.split("EPC RATING")[1].strip().split(" ")[0])
        
        if "lift" in desc.lower():
            item_loader.add_value("elevator", True)
        
        if "no pets" in desc.lower():
            item_loader.add_value("pets_allowed", False)
        
        features = " ".join(response.xpath("//div[@id='features']//text()").getall())
        
        if ("unfurnished" in desc.lower()):
            item_loader.add_value("furnished", False)
        elif ("furnished" in desc.lower()) or ("furnished" in features.lower()):
            item_loader.add_value("furnished", True)
        
        if ("parking" in desc.lower()) or ("parking" in features.lower()):
            item_loader.add_value("parking", True)
        
        if ("washing machine" in desc.lower()) or ("washing machine" in features.lower()):
            item_loader.add_value("washing_machine", True)
        
        images = [x for x in response.xpath("//div[@class='fotorama']/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
            
        item_loader.add_value("landlord_name","Hallam Hills Ltd")
        item_loader.add_value("landlord_phone","0114 327 8853")
        item_loader.add_value("landlord_email","ask@hallamhills.co.uk")
        
        yield item_loader.load_item()

