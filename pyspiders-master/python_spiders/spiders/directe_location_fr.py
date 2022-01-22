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

class MySpider(Spider):
    name = 'directe_location_fr' 
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source='Directe_Location_PySpider_france'
    post_url = "https://www.directe-location.fr/location-bordeaux-gironde.html"
    current_index = 0
    other_prop = ["654"]
    other_type = ["house"]
    custom_settings = {
        "PROXY_ON":True,
        "CONCURRENT_REQUESTS": 4,
        "COOKIES_ENABLED": True,
        "AUTOTHROTTLE_ENABLED": False,
        "RETRY_TIMES": 3,
        "LOG_LEVEL":"INFO",
        "DOWNLOAD_DELAY": 3,
    }
    def start_requests(self):
        formdata = {
            "action": "search",
            "id_ville": "",
            "id_rubrique": "1",
            "carac17[]": "655",
            "prixmax": "",
            "ville": "",
            "distance": "0",
        }
        yield FormRequest(self.post_url,
                        callback=self.parse,
                        formdata=formdata,
                        dont_filter=True,
                        meta={'property_type': "apartment"})

            
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for url in response.xpath("//a[contains(@class,'item item-wide  rub1')]/@href").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, meta={"property_type":response.meta["property_type"], 'purge_cookies': True})
            seen = True
        if page == 2 or seen:
            formdata = {
                "page": str(page),
                "ajaxload": "1",
                "id_rubrique": "1",
            }
            yield FormRequest(self.post_url,
                            callback=self.parse,
                            formdata=formdata,
                            headers={
                                "x-requested-with": "XMLHttpRequest",
                                "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
                                "origin": "https://www.directe-location.fr",
                                
                            },
                            dont_filter=True,
                            meta={'property_type': response.meta["property_type"], "page":page+1, 'purge_cookies': True})
        # elif self.current_index < len(self.other_prop):
        #     formdata = {
        #         "action": "search",
        #         "id_ville": "",
        #         "id_rubrique": "1",
        #         "carac17[]": self.other_prop[self.current_index],
        #         "prixmax": "",
        #         "ville": "",
        #         "distance": "0",
        #     }
        #     yield FormRequest(self.post_url,
        #                     callback=self.parse,
        #                     formdata=formdata,
        #                     dont_filter=True,
        #                     meta={'property_type': self.other_type[self.current_index], 'purge_cookies': True})
        #     self.current_index += 1

                
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        prop_type = response.xpath("//div[contains(@class,'caracteristiques')]//h4//text()[contains(.,'Studio')]").get()
        if prop_type:
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Directe_Location_PySpider_france")
        
        title = response.xpath("//title/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        address = response.xpath("//h3[@class='ville']/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)
        
        rent = response.xpath("//h2[contains(@class,'prix')]/text()").get()
        if rent:
            item_loader.add_value("rent", rent.split("€")[0].replace(" ",""))
        item_loader.add_value("currency", "EUR")
        
        external_id = response.xpath("substring-after(//div[contains(@class,'ref pul')]/text(),':')").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
            
        import dateparser
        available_date = response.xpath("substring-after(//div[@class='maj']/text(),':')").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        description = " ".join(response.xpath("//div[@class='description']//text()").getall())
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", description)
        
        if "chambres" in description:
            room_count = description.split("chambres")[0].strip().split(" ")[-1]
            if room_count.isdigit():
                item_loader.add_value("room_count", str(room_count))
        elif "chambre" in description:
            room_count = description.split("chambre")[0].strip().split(" ")[-1]
            if room_count.isdigit():
                item_loader.add_value("room_count", str(room_count))
        
        if "m2" in description:
            square_meters = description.split("m2")[0].strip().split(" ")[-1]
            if square_meters.replace(",","").replace(".","").isdigit():
                item_loader.add_value("square_meters", int(float(square_meters.replace(",",""))))
        
        images = [response.urljoin(x) for x in response.xpath("//div[@class='carousel-inner']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//script[contains(.,'mapCoord')]/text()").get()
        if latitude_longitude:
            latitude_longitude = latitude_longitude.split('mapCoord')[-1]
            latitude = latitude_longitude.split("LatLng('")[1].split(',')[0]
            longitude = latitude_longitude.split("LatLng('")[1].split(',')[1].split(')')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        item_loader.add_value("landlord_name","DIRECTE LOCATION")
        phone = "".join(response.xpath("//h2[@class='chapo']/text()").getall())
        if phone:
            item_loader.add_value("landlord_phone", phone.replace("."," ").strip())
        
        yield item_loader.load_item()