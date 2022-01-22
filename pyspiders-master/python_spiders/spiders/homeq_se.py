# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.http import headers
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import requests

class MySpider(Spider):
    name = 'homeq_se'
    execution_type = 'testing'
    country = 'sweden' 
    locale ='sv'
    external_source = 'Homeq_PySpider_sweden'
    custom_settings = {
        # "RETRY_HTTP_CODES": [500, 503, 504, 400, 401, 403, 405, 407, 408, 416, 456, 502, 429, 307, 499],
        "PROXY_ON": True,
        "HTTPCACHE_ENABLED": False
    }  
    #     ]  # LEVEL 1
    def start_requests(self):
        # headers = {
        #     "Accept": "application/json, text/plain, */*",
        #     "Accept-Encoding": "gzip, deflate, br",
        #     "Accept-Language": "en,tr-TR;q=0.9,tr;q=0.8,en-US;q=0.7",
        #     "Connection": "keep-alive",
        #     "Content-Length": "72",
        #     "Content-Type": "application/json;charset=UTF-8",
        #     "Host": "search.homeq.se",
        #     "Origin": "https://www.homeq.se",
        #     "Referer": "https://www.homeq.se/",
        #     "sec-ch-ua": '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
        #     "sec-ch-ua-mobile": "?0",
        #     "Sec-Fetch-Dest": "empty",
        #     "Sec-Fetch-Mode": "cors",
        #     "Sec-Fetch-Site": "same-site",
        #     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36"
        # }
        payload = {"shapes":[],"sorting":"publish_date.desc"} 
        p_url = "https://search.homeq.se/api/v2/search"

        yield Request(
            p_url,
            callback=self.parse,
            body=json.dumps(payload),
            #headers=headers,
            method="POST",
            dont_filter=True,
        )

    # 1. FOLLOWING
    def parse(self, response):
        item=json.loads(response.body)
        for item in item["results"]:
            

            if isinstance(item["rent"],list):
                loop_number = len(item["rent"])
                is_list = True
            else:
                loop_number = 1
                is_list = False

            if item["images"]:
                result_img = []
                for img in item["images"]:
                    result_img.append(img["image"])

            json_data2 = requests.get(f"https://www.homeq.se/api/v1/projects/{item['id']}")
            if json_data2.status_code == 200:
                try:
                    data2 = json.loads(json_data2.content)
                    description = data2["info_description"]
                    latitude = data2["project_location"]["latitude"]
                    longitude = data2["project_location"]["longitude"]
                    zipcode = data2["project_location"]["zip_code"]
                except:
                    pass



            
            for index in range(loop_number):
                item_loader = ListingLoader(response=response)
                follow_url = f"https://www.homeq.se/projekt/{item['id']}/"
                item_loader.add_value("external_link", f"{follow_url}#{item['id']}-{index}")
                item_loader.add_value("external_source", self.external_source)
                item_loader.add_value("property_type","apartment")
                item_loader.add_value("external_id",str(item["id"]))
                item_loader.add_value("latitude",str(item["location"]["lat"]))
                item_loader.add_value("longitude", str(item["location"]["lon"]))
                address = item["address"]["municipality"]+" "+item["address"]["county"]
                item_loader.add_value("address",address) 
                item_loader.add_value("landlord_name","HOMEQ")
                item_loader.add_value("landlord_email","support@homeq.se")  
                item_loader.add_value("currency","SEK")
                item_loader.add_value("description",description)
                item_loader.add_value("latitude",latitude)
                item_loader.add_value("longitude",longitude)
                if len(str(zipcode)) > 8:
                    return
                item_loader.add_value("zipcode",zipcode)  
                if item['address'].get("city"):
                    item_loader.add_value("city", item['address']['city'])
                if item["address"].get("zip"):
                    return
                if item["images"]:
                    item_loader.add_value("images",result_img)

                if "name" in item:
                    item_loader.add_value("title", item['name'])
                    item_loader.add_value("city", item['name'])            

                if is_list:
                    item_loader.add_value("room_count",item["rooms"][index])
                    item_loader.add_value("rent",item["rent"][index])
                    item_loader.add_value("floor",str(item["floor"][index]))

                    if item["access_date"]:
                        item_loader.add_value("available_date",item["access_date"][index])

                    if item["status"]:
                        status = item["status"][index]
                        if status == "Reserved":
                            yield

                    item_loader.add_value("parking",item["amenities"]["parking"][index])
                    item_loader.add_value("elevator",item["amenities"]["elevator"][index])
                    item_loader.add_value("balcony",item["amenities"]["balcony"][index])
                    if item["amenities"]["pets"]:
                        item_loader.add_value("pets_allowed",item["amenities"]["pets"][index])
                    item_loader.add_value("dishwasher",item["amenities"]["dishwasher"][index])
                    item_loader.add_value("washing_machine",item["amenities"]["washer"][index])     
                    area = str(item["area"][index])
                    area = area.split(".")[0]
                    item_loader.add_value("square_meters",area)



                    yield item_loader.load_item()               

                else:
                    item_loader.add_value("room_count",item["rooms"])
                    item_loader.add_value("rent",item["rent"])
                    item_loader.add_value("floor",str(item["floor"]))
                    if item["images"]:
                        item_loader.add_value("images",result_img)

                    if item["access_date"]:
                        item_loader.add_value("available_date",item["access_date"])

                    status = item["status"]

                    if status == "Reserved":
                        return

                    item_loader.add_value("parking",item["amenities"]["parking"])
                    item_loader.add_value("elevator",item["amenities"]["elevator"])
                    item_loader.add_value("balcony",item["amenities"]["balcony"])
                    if item["amenities"]["pets"]:
                        item_loader.add_value("pets_allowed",item["amenities"]["pets"])
                    item_loader.add_value("dishwasher",item["amenities"]["dishwasher"])
                    item_loader.add_value("washing_machine",item["amenities"]["washer"])     

                    area = str(item["area"])
                    area = area.split(".")[0]
                    item_loader.add_value("square_meters",area)
                       
                    
                    yield item_loader.load_item()





                

            






    def get_infos(self, response):
        data = json.loads(response.body)
        item_loader = response.meta.get('item_loader')
        description = data["info_description"]
        item_loader.add_value("description", description)
        yield item_loader.load_item()