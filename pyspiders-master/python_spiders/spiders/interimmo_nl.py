# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from datetime import timedelta
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re
from scrapy.http import HtmlResponse


class MySpider(Spider):
    name = 'interimmo_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    url = "https://www.interimmo.nl/wp-admin/admin-ajax.php"

    download_timeout = 120
    custom_settings = {
        "CONCURRENT_REQUESTS": 4,    
        "COOKIES_ENABLED": False,    
        "AUTOTHROTTLE_ENABLED": True,    
        "AUTOTHROTTLE_START_DELAY": .5,    
        "AUTOTHROTTLE_MAX_DELAY": 2,    
        "RETRY_TIMES": 3,           
        "DOWNLOAD_DELAY": 0,
    }
    

    headers = {
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36",
        "origin": "https://www.interimmo.nl"
    }
           
    data = {
        "action": "houzez_half_map_listings",
        "keyword": "",
        "location": "all",
        "status":"Te huur",
        "bedrooms": "",
        "min_price": "€0",
        "max_price": "€2.000.000",
        "use_radius": "off",
        "security": "704f5136d0",
        "paged": "0",
        "post_per_page": "20",
        
    }
    def start_requests(self):
        yield FormRequest(
            url=self.url,
            formdata=self.data,
            headers=self.headers,
            callback=self.parse,
        )
    
    # 1. FOLLOWING
    def parse(self, response):
        
        data = json.loads(response.body)

        headers = {
            # "accept" : "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            # "accept-encoding" : "gzip, deflate, br",
            # "accept-language" : "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
            # "referer" : "https://www.interimmo.nl/aanbod/woning-huren/",
            "user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36"
        }
        
        page = response.meta.get('page', 1)
        seen = False
        try:
            for item in data["properties"]:
                follow_url = item["url"]
                external_id = item['id']
                lat = item["lat"] 
                lng = item["lng"]
                prop_type = item["type"]
                title = item["title"]
                address = item["address"]
                room_count = item["bedrooms"]
                bathroom_count = item["bathrooms"]
                rent = item["price"]
                square_meters = item["prop_meta"]
                images = item['thumbnail']
                #{'': 'OK', 'Appartement': 'OK', 'Eengezinswoning': 'OK', 'Eengezinswoning, Hoekwoning': 'OK', 'villa': 'OK'}
                if prop_type and "Appartement" in prop_type:
                    prop_type = "apartment"
                elif prop_type and ("Eengezinswoning" or "Hoekwoning" or "villa") in prop_type:
                    prop_type = "house"
                else:
                    prop_type = None
                if prop_type != "" and room_count != "":
                    yield Request(follow_url, headers=headers, callback=self.populate_item, meta={"lat": lat, "lng": lng, "prop_type":prop_type, "address":address,
                    "room_count":room_count, "bathroom_count":bathroom_count, "title":title, "external_id":external_id, "rent":rent, "square_meters":square_meters,
                    "images":images, "follow_url":follow_url})
                seen = True
            
            if page==1 or seen:
                self.data["paged"] = str(page)
                yield FormRequest(
                    url = self.url,
                    formdata=self.data,
                    headers=self.headers,
                    callback=self.parse,
                    meta={"page": page+1}

                )
        except:
            pass


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        rented = response.xpath("//div[@id='gallery']/span[contains(@class,'label-wrap')]/span[.='Verhuurd']//text()").extract_first()
        if rented:
            return
            
        item_loader.add_value("external_source", "Interimmo_PySpider_" + self.country + "_" + self.locale)
        item_loader.add_value("external_id", str(response.meta.get("external_id")))
        
        lat = response.meta.get("lat")
        lng = response.meta.get("lng")
        address = response.meta.get('address')
        prop_type = response.meta.get("prop_type")
        title = response.meta.get("title")
        room_count = response.meta.get("room_count")
        bathroom_count = response.meta.get("bathroom_count")
        if prop_type:
            item_loader.add_value("property_type",prop_type)
        else:
            item_loader.add_value("property_type", "house")
        
        rent = response.meta.get('rent')
        if rent:
            item_loader.add_value("rent", re.sub(r"\D", "", rent))
            item_loader.add_value("currency", "EUR")

        square_meters = response.meta.get("square_meters")
        if square_meters:
            item_loader.add_value("square_meters", re.search(r"size:\s(\d+)\sm2", square_meters))
        item_loader.add_value("latitude",lat)
        item_loader.add_value("longitude",lng)
        item_loader.add_value("title",title)
        item_loader.add_value("address", address)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("external_link", response.url)
        
        
        desc = "".join(response.xpath("//div[@id='description']/p//text()").extract())

        if desc:
            item_loader.add_value("description", desc.strip())
            if "gemeubileerd" in desc:
                item_loader.add_value("furnished", True)
            if "Huisdieren zijn niet toegestaan" in desc:
                item_loader.add_value("pets_allowed", False)
            if "parkeergarage" in desc:
                item_loader.add_value("parking", True)
        images = response.meta.get("images")
        image_extract = re.findall(r"https.*jpg", images)
        if image_extract:
                item_loader.add_value("images", list(set(images)))
        furnished = response.xpath("//span[@class='item-price']/text()[contains(.,'gemeubileerd')]").extract_first()
        if furnished:
            item_loader.add_value("furnished", True)
        elif "gemeubileerd" in desc.lower():
            item_loader.add_value("furnished", True)
        available_date = response.xpath("//div[@id='description']//p//text()[contains(.,'beschikbaar per')]").get()
        if available_date:
            try:
                new_format = available_date.split("beschikbaar per")[1].strip().split("voor")[0]           
                new_date = dateparser.parse(new_format.strip(), languages=['nl']).strftime("%Y-%m-%d")
                item_loader.add_value("available_date", new_date)
            except:
                pass

        
        if "," in address:
            zipcode = address.split(",")[1].split(",")[0]
            item_loader.add_value("zipcode", zipcode.strip())
            item_loader.add_value("city", address.split(zipcode)[1].replace(",","").strip())


        item_loader.add_value("landlord_phone", "020 − 33 17 567")
        item_loader.add_value("landlord_email", "amsterdam@interimmo.nl")
        item_loader.add_value("landlord_name", "Interimmo")
        yield item_loader.load_item()