# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'haart_co_uk'
    external_source = "HaartChelsea_PySpider_united_kingdom"
    execution_type = "test"
    country = "united_kingdom"
    locale = "en"
    post_urls = 'https://www.haart.co.uk/umbraco/api/PropertySearch/FindNearestSRProperties'
    
    payload = {"Lat": 51.4878, "Lon": -0.16788, "Brand": "HRT", "IsPurchase": False, "Distance": "5miles", "Sort": "distance"}
    
    headers = {
        'accept': 'application/json, text/plain, */*',
        'content-type': 'application/json',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36',
        'accept-language': 'tr,en;q=0.9,tr-TR;q=0.8,en-US;q=0.7,es;q=0.6,fr;q=0.5,nl;q=0.4',
    }
    
    def start_requests(self):
        yield Request(
            self.post_urls,
            callback=self.parse,
            body=json.dumps(self.payload),
            headers=self.headers,
            method="POST"
        )
    
    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)["Results"]
        for item in data:
            follow_url = response.urljoin(item["SeoUrl"])
            if get_p_type_string(item["PropertyTypeText"]) and not item["IsLetAgreed"]:
                url = f"https://www.haart.co.uk/umbraco/api/PropertySearch/GetProperty?propRef={follow_url.split('/')[-1]}"
                headers = {
                    'accept': 'application/json, text/plain, */*',
                    'content-type': 'application/json',
                    'authorization': '45245751164315531324614523545354134546435448533464312464312354566854223234641455721354135412345132465413544123451654464531646564515451516454845465466548454652356714131354513587941231361264612348131512454644154235643513546978644612346135456454311326464613036454',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36',
                    'accept-language': 'tr,en;q=0.9,tr-TR;q=0.8,en-US;q=0.7,es;q=0.6,fr;q=0.5,nl;q=0.4',
                }
                yield Request(
                    url,
                    callback=self.populate_item,
                    headers=headers,
                    meta={
                        "property_type": get_p_type_string(item["PropertyTypeText"]),
                        "base_url": follow_url,
                        "data": item
                    }
                )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.meta.get('base_url'))
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        item = json.loads(response.body)
        data = response.meta.get('data')

        status=data["SellingPrice"]
        if status and status==0:
            return
        status1 = "".join(response.meta.get('base_url'))
        if status1 and "buying" in status1.lower():
            return
        title=str(item["PropertyTypeText"])+" "+str(item["PropertySummary"])
        if title:
            item_loader.add_value("title", title)
        else:
            title=item["WebDescription"]
            if title:
                item_loader.add_value("title", title)    
        item_loader.add_value("room_count", data["Bedrooms"])
        item_loader.add_value("rent", data["SellingPrice"])
        item_loader.add_value("currency", "GBP")
        
        item_loader.add_value("address", item["ShortAddress"])
        item_loader.add_value("city", item["Town"])
        item_loader.add_value("zipcode", item["PostCode"])

        description=data["LongDescription"] 
        if description:       
            item_loader.add_value("description", description)

            if "bathroom" in description.lower():
                if "one bathroom" in description.lower():
                    item_loader.add_value("bathroom_count", "1")
                elif "two bathroom" in description.lower():
                    item_loader.add_value("bathroom_count", "2")
                elif "three bathroom" in description.lower():
                    item_loader.add_value("bathroom_count", "3")
                elif "four bathroom" in description.lower():
                    item_loader.add_value("bathroom_count", "4")
                else:
                    item_loader.add_value("bathroom_count", "1")


        item_loader.add_value("latitude", str(data["Location"]["lat"]))
        item_loader.add_value("longitude", str(data["Location"]["lon"]))
        
        for i in data["Images"]:
            images = f"https://propimage.blob.core.windows.net/propimage/mdres/{i['Filename']}.jpg?width=329&mode=max"
            item_loader.add_value("images", images)
    
        import dateparser
        available_date = item["AvailableDate"]
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        item_loader.add_value("external_id", item["ReferenceId"])
        
        features = item["Characteristics"]
        for i in features:
            if "balcon" in i["Description"].lower():
                item_loader.add_value("balcony", True)
            if "unfurnished" not in i["Description"].lower() and "furnished" in i["Description"].lower():
                item_loader.add_value("furnished", True)
            if "dishwasher" in i["Description"].lower():
                item_loader.add_value("dishwasher", True)
            if "parking" in i["Description"].lower() or "garage" in i["Description"].lower():
                item_loader.add_value("parking", True)
            if "pet friendly" in i["Description"].lower():
                item_loader.add_value("pets_allowed", True)
            if "lift" in i["Description"].lower():
                item_loader.add_value("elevator", True)    
        
        floor_plan_images = item["Floorplans"]
        for i in floor_plan_images:
            images = f"https://propimage.blob.core.windows.net/propimage/mdres/{i['Filename']}.jpg?width=329&mode=max"
            item_loader.add_value("floor_plan_images", i['Filename'])

        item_loader.add_value("landlord_email", "clapham.common.lettings@haart.co.uk")
        item_loader.add_value("landlord_name", "haart Clapham Common")
        item_loader.add_value("landlord_phone", "020 7498 2133")

    #     url = f"https://www.haart.co.uk/umbraco/api/AngularAPI/GetOfficeDetails?officeID=0405"
    #     yield Request(url, callback=self.get_landlord, headers=self.headers, meta={"item_loader": item_loader})

    # def get_landlord(self, response):
    #     data = json.loads(response.body)
    #     item_loader = response.meta.get('item_loader')

    #     landlord_email = data["LettingsEmail"]
    #     if landlord_email:
    #         item_loader.add_value("landlord_email", landlord_email)

    #     landlord_name = data["OfficeName"]
    #     if landlord_name:
    #         item_loader.add_value("landlord_name", landlord_name)

    #     landlord_phone = data["TelNo"]
    #     if landlord_phone:
    #         item_loader.add_value("landlord_phone", landlord_phone)

        yield item_loader.load_item()
    
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "cottage" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None