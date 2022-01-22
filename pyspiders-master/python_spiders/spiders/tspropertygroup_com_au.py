# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser

class MySpider(Spider):
    name = 'tspropertygroup_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'
    start_urls = ['https://clientapi.prolist.net.au/api/listings/search?embed=false']  # LEVEL 1
    payload = '{"SearchLevel":3,"SearchGuid":"4c69b016-a1f0-4577-aaea-80d4db82a9a4","SearchGuids":[],"Page":1,"PageSize":9,"ExcludeConjunctionalListings":false,"IsHighlightListing":false,"IsFeatured":false,"IsPROListFeatured":false,"HaveInspections":false,"PropertyId":"","Estate":"","Categories":[1],"Statuses":[0,1],"PropertyTypes":[],"PropertyCategories":[],"PropertySubCategories":[],"MethodsOfSale":[],"AddressString":"","LotNumber":"","UnitNumber":"","StreetNumber":"","Suburbs":[],"BoundsNorth":"","BoundsEast":"","BoundsSouth":"","BoundsWest":"","OrderByStatements":[],"MinBedrooms":0,"MinBathrooms":0,"MinParking":"","MinLandArea":"","MaxLandArea":"","MinPrice":"","MaxPrice":"","MinSoldPrice":"","MaxSoldPrice":""}'
    custom_settings = {
        "PROXY_ON":"True",
        "PASSWORD": "wmkpu9fkfzyo",
    } 
    headers = {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "tr,en;q=0.9,tr-TR;q=0.8,en-US;q=0.7,es;q=0.6,fr;q=0.5,nl;q=0.4",
        "content-type": "application/json",
        "origin": "https://www.tspropertygroup.com.au",
        "referer": "https://www.tspropertygroup.com.au/",
        "sec-ch-ua": '"Google Chrome";v="89", "Chromium";v="89", ";Not A Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "cross-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36",
        "x-prolist-client-website-id": "75fe2d66-8b74-4a5f-91ba-a9179de77dfc",
        "x-prolist-website-id": "4c69b016-a1f0-4577-aaea-80d4db82a9a4",
        "x-prolist-website-level": "3",
        }
    
    def start_requests(self):
        
        yield Request(
            url=self.start_urls[0],
            method="POST",
            dont_filter=True,
            callback=self.parse,
            headers=self.headers,
            body=self.payload
        )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen = False
        
        data = json.loads(response.body)
        for item in data["Items"]:
            follow_url = f"https://www.tspropertygroup.com.au/listings/{item['Id']}"
            prop_type = item["PropertyType"]
            if get_p_type_string(prop_type):
                prop_type = get_p_type_string(prop_type)
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": prop_type,"item":item})
            seen = True
            
        if page == 1 or seen:
            self.payload = self.payload.replace(f'"Page":{page-1}', f'"Page":{page}')
            yield Request(
                url=self.start_urls[0],
                method="POST",
                dont_filter=True,
                callback=self.parse,
                headers=self.headers,
                body=self.payload,
                meta={"page":page+1}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item = response.meta.get('item')
        item_loader.add_value("title", item["Header"])
        item_loader.add_value("external_id", str(item["PropertyId"]))
        item_loader.add_value("external_source", "Tspropertygroup_Com_PySpider_australia")
        if "Agents" in item:
            item_loader.add_value("landlord_name", item["Agents"][0]["FullName"])
            item_loader.add_value("landlord_phone", item["Agents"][0]["OfficePhone"])
            item_loader.add_value("landlord_email", item["Agents"][0]["Email"])
        if "Address" in item:
            item_loader.add_value("address", item["Address"]["FullAddress"])
            item_loader.add_value("zipcode", item["Address"]["PostCode"])
            item_loader.add_value("city", item["Address"]["Suburb"])
            item_loader.add_value("latitude", str(item["Address"]["Coordinates"]["Lat"]))
            item_loader.add_value("longitude", str(item["Address"]["Coordinates"]["Lon"]))
        if "Price" in item:
            if "pw" in item["Price"]["Price"]:
                rent = "".join(filter(str.isnumeric, item["Price"]["Price"].split('.')[0].replace(',', '').replace('\xa0', '')))
                item_loader.add_value("rent", str(int(float(rent)*4)))
                item_loader.add_value("currency", "AUD")
            else:
                rent = "".join(filter(str.isnumeric, item["Price"]["Price"].split('.')[0].replace(',', '').replace('\xa0', '')))
                item_loader.add_value("rent_string", rent)     
            if "BondPrice" in item["Price"]:
                item_loader.add_value("deposit", item["Price"]["BondPrice"])     
        item_loader.add_value("room_count", item["Features"]["Bedrooms"])     
        item_loader.add_value("bathroom_count", item["Features"]["Bathrooms"])    
        parking = item["Features"]["Parking"]["Total"]
        if parking != 0:
            item_loader.add_value("parking", True)     
        else:
            item_loader.add_value("parking", False)     
        furnished = item["Features"]["IsFurnished"]
        if furnished: item_loader.add_value("furnished", True)     
        else: item_loader.add_value("furnished", False)   
        
        balcony = item["Features"]["HasBalcony"]
        if balcony: item_loader.add_value("balcony", True)     
        else: item_loader.add_value("balcony", False)   

        dishwasher = item["Features"]["HasDishwasher"]
        if dishwasher: item_loader.add_value("dishwasher", True)     
        else: item_loader.add_value("dishwasher", False)   

        swimming_pool = item["Features"]["HasPoolInGround"]
        if swimming_pool: item_loader.add_value("swimming_pool", True)     
        else: item_loader.add_value("swimming_pool", False)   

        description = item["WebDescription"]   
        if description:
            item_loader.add_value("description", description.strip())
        available_date = item["DateAvailable"]  
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"], languages=['en'])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
           
        images = [x["Large"]["Url"] for x in item["Images"]]
        if images:
            item_loader.add_value("images", images)

        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "terrace" in p_type_string.lower() or "detached" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    else:
        return None