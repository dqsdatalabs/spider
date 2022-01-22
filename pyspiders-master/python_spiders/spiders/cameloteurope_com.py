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
    name = 'cameloteurope_com'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'

    custom_settings = {
        "PROXY_ON" : True,
        'CONCURRENT_REQUESTS':10,
        'HTTPCACHE_ENABLED':True,
        'DOWNLOAD_DELAY':5,
        'CONCURRENT_REQUESTS_PER_IP':10,
    }
    
    headers = {
        'Content-Type': 'application/json'
    }
    def start_requests(self):
        
        payload="{\r\n    \"operationName\": \"Advertisements\",\r\n    \"variables\": {\r\n        \"city\": \"\",\r\n        \"contractType\": \"RENT_CONTRACT\",\r\n        \"country\": \"\",\r\n        \"entity\": \"\",\r\n        \"locale\": \"nl\",\r\n        \"from\": 0,\r\n        \"size\": 6,\r\n        \"unitType\": [\r\n            \"LONG_STAY\",\r\n            \"OZ_STUDIO_01\",\r\n            \"OZ_STUDIO_02\",\r\n            \"OZ_STUDIO_03\",\r\n            \"OZ_STUDIO_04\",\r\n            \"OZ_STUDIO_05\",\r\n            \"OZ_STUDIO_07\",\r\n            \"OZ_STUDIO_08\",\r\n            \"OZ_STUDIO_09\",\r\n            \"OZ_STUDIO_10\",\r\n            \"OZ_STUDIO_11\",\r\n            \"OZ_STUDIO_12\",\r\n            \"OZ_STUDIO_13\",\r\n            \"OZ_STUDIO_14\",\r\n            \"OZ_STUDIO_15\",\r\n            \"RESIDENTIAL\",\r\n            \"ROOM\",\r\n            \"ROOM_01\",\r\n            \"ROOM_02\",\r\n            \"ROOM_03\",\r\n            \"ROOM_04\",\r\n            \"ROOM_05\",\r\n            \"ROOM_07\",\r\n            \"ROOM_08\",\r\n            \"ROOM_09\",\r\n            \"SHORT_STAY\",\r\n            \"STUDIO_02\",\r\n            \"STUDIO_02\",\r\n            \"STUDIO_02\",\r\n            \"STUDIO_01\",\r\n            \"STUDIO_03\",\r\n            \"STUDIO_04\",\r\n            \"STUDIO_05\",\r\n            \"STUDIO_06\",\r\n            \"STUDIO_07\",\r\n            \"STUDIO_08\",\r\n            \"STUDIO_09\",\r\n            \"STUDIO_10\",\r\n            \"STUDIO_11\",\r\n            \"STUDIO_12\",\r\n            \"STUDIO_13\",\r\n            \"STUDIO_14\",\r\n            \"STUDIO_15\",\r\n            \"STUDIO_16\",\r\n            \"STUDIO_17\",\r\n            \"STUDIO_18\",\r\n            \"STUDIO_19\",\r\n            \"STUDIO_20\",\r\n            \"STUDIO_21\",\r\n            \"STUDIO_22\",\r\n            \"STUDIO_23\",\r\n            \"STUDIO_24\",\r\n            \"STUDIO_25\",\r\n            \"STUDIO_26\",\r\n            \"STUDIO_27\",\r\n            \"STUDIO_28\",\r\n            \"STUDIO_29\",\r\n            \"STUDIO_30\",\r\n            \"STUDIO_31\",\r\n            \"STUDIO_32\",\r\n            \"STUDIO_33\",\r\n            \"STUDIO_34\",\r\n            \"STUDIO_35\",\r\n            \"STUDIO_36\",\r\n            \"STUDIO_37\",\r\n            \"STUDIO_38\",\r\n            \"STUDIO_39\",\r\n            \"STUDIO_40\",\r\n            \"STUDIO_48\",\r\n            \"STUDIO_49\",\r\n            \"STUDIO_41\",\r\n            \"STUDIO_42\",\r\n            \"STUDIO_43\",\r\n            \"STUDIO_44\",\r\n            \"STUDIO_45\",\r\n            \"STUDIO_46\",\r\n            \"STUDIO_47\",\r\n            \"STUDIO_50\",\r\n            \"STUDIO_51\",\r\n            \"STUDIO_52\",\r\n            \"STUDIO_53\",\r\n            \"STUDIO_54\",\r\n            \"STUDIO_55\",\r\n            \"STUDIO_56\",\r\n            \"STUDIO_57\",\r\n            \"STUDIO_58\",\r\n            \"STUDIO_59\",\r\n            \"UU_STUDIO_01\",\r\n            \"UU_STUDIO_02\"\r\n        ],\r\n        \"onlyPublished\": true\r\n    },\r\n    \"query\": \"query Advertisements($country: String, $city: String, $contractType: String, $entity: String, $id: String, $ids: [String], $locale: String, $from: Int, $size: Int, $unitType: [String], $onlyPublished: Boolean) {\\n  advertisements(\\n    country: $country\\n    city: $city\\n    unitType: $unitType\\n    contractType: $contractType\\n    entity: $entity\\n    id: $id\\n    ids: $ids\\n    locale: $locale\\n    from: $from\\n    size: $size\\n    onlyPublished: $onlyPublished\\n  ) {\\n    id\\n    advertisementId\\n    entityCode\\n    entityDescription\\n    buildingReference\\n    title\\n    content\\n    address\\n    zipcode\\n    city\\n    startingPrice\\n    squareFootage\\n    subState\\n    name\\n    targetGroup\\n    unitType\\n    occupancyNumber\\n    furnishing\\n    facilities\\n    contractType\\n    contractTemplate\\n    numberOfVacancies\\n    minDuration\\n    maxDuration\\n    monthlyCosts {\\n      category\\n      amount\\n      __typename\\n    }\\n    onetimeCosts {\\n      category\\n      amount\\n      __typename\\n    }\\n    optinCosts {\\n      chargecode\\n      amount\\n      __typename\\n    }\\n    deposit\\n    maxPrice\\n    mainImage\\n    images\\n    total\\n    totalQuery\\n    __typename\\n  }\\n}\\n\"\r\n}"
        p_url = "https://sa.camelotrooms.com/"
        yield Request(
            p_url,
            callback=self.parse,
            body=payload,
            headers=self.headers,
            method="POST",
        )

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 6)
        seen = False
        data = json.loads(response.body)
        for item in data["data"]["advertisements"]:
            p_type = item["unitType"]
            if get_p_type_string(p_type):
                p_type = get_p_type_string(p_type)
            else:
                continue
            follow_url = f"https://spots4you.com/nl/nl/detail/{item['id']}/{item['name']}"
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":p_type})
        
        if page == 2 or seen:
            p_url = "https://sa.camelotrooms.com/"
            payload="{\r\n    \"operationName\": \"Advertisements\",\r\n    \"variables\": {\r\n        \"city\": \"\",\r\n        \"contractType\": \"RENT_CONTRACT\",\r\n        \"country\": \"\",\r\n        \"entity\": \"\",\r\n        \"locale\": \"nl\",\r\n        \"from\": " + str(page) + ",\r\n        \"size\": 6,\r\n        \"unitType\": [\r\n            \"LONG_STAY\",\r\n            \"OZ_STUDIO_01\",\r\n            \"OZ_STUDIO_02\",\r\n            \"OZ_STUDIO_03\",\r\n            \"OZ_STUDIO_04\",\r\n            \"OZ_STUDIO_05\",\r\n            \"OZ_STUDIO_07\",\r\n            \"OZ_STUDIO_08\",\r\n            \"OZ_STUDIO_09\",\r\n            \"OZ_STUDIO_10\",\r\n            \"OZ_STUDIO_11\",\r\n            \"OZ_STUDIO_12\",\r\n            \"OZ_STUDIO_13\",\r\n            \"OZ_STUDIO_14\",\r\n            \"OZ_STUDIO_15\",\r\n            \"RESIDENTIAL\",\r\n            \"ROOM\",\r\n            \"ROOM_01\",\r\n            \"ROOM_02\",\r\n            \"ROOM_03\",\r\n            \"ROOM_04\",\r\n            \"ROOM_05\",\r\n            \"ROOM_07\",\r\n            \"ROOM_08\",\r\n            \"ROOM_09\",\r\n            \"SHORT_STAY\",\r\n            \"STUDIO_02\",\r\n            \"STUDIO_02\",\r\n            \"STUDIO_02\",\r\n            \"STUDIO_01\",\r\n            \"STUDIO_03\",\r\n            \"STUDIO_04\",\r\n            \"STUDIO_05\",\r\n            \"STUDIO_06\",\r\n            \"STUDIO_07\",\r\n            \"STUDIO_08\",\r\n            \"STUDIO_09\",\r\n            \"STUDIO_10\",\r\n            \"STUDIO_11\",\r\n            \"STUDIO_12\",\r\n            \"STUDIO_13\",\r\n            \"STUDIO_14\",\r\n            \"STUDIO_15\",\r\n            \"STUDIO_16\",\r\n            \"STUDIO_17\",\r\n            \"STUDIO_18\",\r\n            \"STUDIO_19\",\r\n            \"STUDIO_20\",\r\n            \"STUDIO_21\",\r\n            \"STUDIO_22\",\r\n            \"STUDIO_23\",\r\n            \"STUDIO_24\",\r\n            \"STUDIO_25\",\r\n            \"STUDIO_26\",\r\n            \"STUDIO_27\",\r\n            \"STUDIO_28\",\r\n            \"STUDIO_29\",\r\n            \"STUDIO_30\",\r\n            \"STUDIO_31\",\r\n            \"STUDIO_32\",\r\n            \"STUDIO_33\",\r\n            \"STUDIO_34\",\r\n            \"STUDIO_35\",\r\n            \"STUDIO_36\",\r\n            \"STUDIO_37\",\r\n            \"STUDIO_38\",\r\n            \"STUDIO_39\",\r\n            \"STUDIO_40\",\r\n            \"STUDIO_48\",\r\n            \"STUDIO_49\",\r\n            \"STUDIO_41\",\r\n            \"STUDIO_42\",\r\n            \"STUDIO_43\",\r\n            \"STUDIO_44\",\r\n            \"STUDIO_45\",\r\n            \"STUDIO_46\",\r\n            \"STUDIO_47\",\r\n            \"STUDIO_50\",\r\n            \"STUDIO_51\",\r\n            \"STUDIO_52\",\r\n            \"STUDIO_53\",\r\n            \"STUDIO_54\",\r\n            \"STUDIO_55\",\r\n            \"STUDIO_56\",\r\n            \"STUDIO_57\",\r\n            \"STUDIO_58\",\r\n            \"STUDIO_59\",\r\n            \"UU_STUDIO_01\",\r\n            \"UU_STUDIO_02\"\r\n        ],\r\n        \"onlyPublished\": true\r\n    },\r\n    \"query\": \"query Advertisements($country: String, $city: String, $contractType: String, $entity: String, $id: String, $ids: [String], $locale: String, $from: Int, $size: Int, $unitType: [String], $onlyPublished: Boolean) {\\n  advertisements(\\n    country: $country\\n    city: $city\\n    unitType: $unitType\\n    contractType: $contractType\\n    entity: $entity\\n    id: $id\\n    ids: $ids\\n    locale: $locale\\n    from: $from\\n    size: $size\\n    onlyPublished: $onlyPublished\\n  ) {\\n    id\\n    advertisementId\\n    entityCode\\n    entityDescription\\n    buildingReference\\n    title\\n    content\\n    address\\n    zipcode\\n    city\\n    startingPrice\\n    squareFootage\\n    subState\\n    name\\n    targetGroup\\n    unitType\\n    occupancyNumber\\n    furnishing\\n    facilities\\n    contractType\\n    contractTemplate\\n    numberOfVacancies\\n    minDuration\\n    maxDuration\\n    monthlyCosts {\\n      category\\n      amount\\n      __typename\\n    }\\n    onetimeCosts {\\n      category\\n      amount\\n      __typename\\n    }\\n    optinCosts {\\n      chargecode\\n      amount\\n      __typename\\n    }\\n    deposit\\n    maxPrice\\n    mainImage\\n    images\\n    total\\n    totalQuery\\n    __typename\\n  }\\n}\\n\"\r\n}"
            yield Request(
                p_url,
                callback=self.parse,
                body=payload,
                headers=self.headers,
                method="POST",
                meta={"page":page+1}
            )
        



     # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        if response.url == "https://spots4you.com/nl/nl/detail/1193a78c-bac7-4c82-9d62-08d88d3b3a85/bomansplaats-30---120":
            return
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Cameloteurope_PySpider_netherlands")
        item_loader.add_xpath("title", "//title/text()") 

        rent = response.xpath("//span[@class='space-specs-price']//text()").get()
        if rent:
            price = rent.split(".")[0].strip()
            item_loader.add_value("rent_string",price.strip())
        else:
            item_loader.add_value("currency","EUR") 

        room_count = "".join(response.xpath("//div[span[.='Aantal ruimtes beschikbaar']]/span[2]/span/text()").getall())
        if room_count:
            item_loader.add_value("room_count",room_count.strip())  

        meters = response.xpath("//div[span[.='Grootte']]/span[2]/span/text()").get()
        if meters:
            s_meters = meters.split("m")[0].replace(",",".").strip()
            item_loader.add_value("square_meters",int(float(s_meters)))

        deposit = response.xpath("//div[@class='detail-spec-row']/span[contains(.,'Borg')]/following-sibling::span/span[@class='content-body']/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.strip())

        utilities = response.xpath("//div[span[.='Servicekosten']]/span[2]/span[2]/text()").get()
        if deposit:
            item_loader.add_value("utilities",utilities.split(",")[0].strip())

        address = "".join(response.xpath("//div[@class='details-hero-content']/h2/text()").getall()) 
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address.strip()))

        city = "".join(response.xpath("substring-after(//div[@class='details-hero-content']/h2/text(),'- ')").getall()) 
        if city:
            item_loader.add_value("city",city.split(",")[-1].strip())
            item_loader.add_value("zipcode",city.split(",")[0].strip())

        description = " ".join(response.xpath("//section[@class='details-info-description']/p/text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', '').strip())

        images = [x for x in response.xpath("//div[@class='details-hero-inner']/img/@src").extract()]
        if images is not None:
            item_loader.add_value("images", images) 

        furnished = " ".join(response.xpath("//div[span[.='Stoffering en meubilering']]/span[2]/span/text()").getall())   
        if furnished:
            item_loader.add_value("furnished", True)

        item_loader.add_value("landlord_name", "Cameloteurope")

        
        yield item_loader.load_item()



     
    

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("bovenwoning" in p_type_string.lower() or "tussenwoning" in p_type_string.lower() or "hoekwoning" in p_type_string.lower()):
        return "house"
    elif p_type_string and "woning" in p_type_string.lower():
        return "house"
    else:
        return None