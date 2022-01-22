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
import dateparser

class MySpider(Spider):
    name = 'unicomproperty_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ['https://unicomproperty.co.uk/properties']  # LEVEL 1

    def start_requests(self):
        yield Request(
            url=self.start_urls[0],
            callback=self.parse,
        )
    # 1. FOLLOWING
    def parse(self, response):
        headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "accept-encoding": "gzip, deflate, br",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.85 Safari/537.36"
        }
        for item in response.xpath("//a[contains(@class,'PropertiesListCard__link')]/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
        next_page = response.xpath("//div[@class='PropertiesPagination__next']/a/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                headers=headers)
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Unicomproperty_Co_PySpider_united_kingdom")
        item_loader.add_xpath("title", "//h1/text()")
        desc = "".join(response.xpath("//p[@class='SplitBlock__text']//text()[normalize-space()]").getall())
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else: 
            desc = "".join(response.xpath("//meta[@name='description']/@content").getall())
            if get_p_type_string(desc):
                item_loader.add_value("property_type", get_p_type_string(desc))
            else: 
                return
        # rent = response.xpath("//h4[@class='PropertyHero__price']/text()[contains(.,'£')]").get()
        # if rent:
        #     rent = rent.split('£')[-1].strip().replace(',', '').replace('\xa0', '')
        #     item_loader.add_value("rent", str(int(float(rent)*4)))    
        item_loader.add_value("currency", "GBP")    

        available_date = response.xpath("//h4/text()[contains(.,'from')]").get()
        if available_date:
            
            available_date = available_date.strip().split(" ")[-1]
            print(available_date)
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        json_data = response.xpath("//script[@type='application/json']//text()").get()
        if json_data:
            data = json.loads(json_data)["props"]["property"]
            
            # item_loader.add_value("external_id",str( data["id"]))
            address = data["acf"]["address"]
            item_loader.add_value("address", address["propertyNumber"]+" "+address["roadName"]+", "+address["city"])
            item_loader.add_value("zipcode", address["postcode"])
            item_loader.add_value("city", address["city"])

            rent = data["acf"]["contracts"][0]["contract"]["prices"][0]["pricePerPersonPerWeek"]
            if rent:
                item_loader.add_value("rent", str(int(float(rent))*4))
            
            item_loader.add_value("latitude", str(data["acf"]["coordinates"]["lat"]))
            item_loader.add_value("longitude", str(data["acf"]["coordinates"]["lng"]))
            if data["acf"]["media"]["photos"]:
                images = [x["url"] for x in data["acf"]["media"]["photos"]]
                if images:
                    item_loader.add_value("images", images)
            try:
                for features in data["acf"]["facilities"]:
                    if "Parking" in features["facility"]:
                        item_loader.add_value("parking", True)
                    if "Washing Machine" in features["facility"]:
                        item_loader.add_value("washing_machine", True)
                    if "Dishwasher" in features["facility"]:
                        item_loader.add_value("dishwasher", True)
                    if "Terrace" in features["facility"]:
                        item_loader.add_value("terrace", True)
            except:
                pass
            desc =data["acf"]["description"]
            item_loader.add_value("description", desc)
            if "bedrooms" in desc:
                room_count = desc.split("bedrooms")[0].replace("Double","").strip().replace("\n"," ").split(" ")[-1]
                if room_count.isdigit():
                    item_loader.add_value("room_count", room_count)

            item_loader.add_value("bathroom_count", data["acf"]["bathrooms"])

        item_loader.add_value("landlord_phone", "+44 (0)1482 491009")
        item_loader.add_value("landlord_email", "amanda@unicomproperty.co.uk")
        item_loader.add_value("landlord_name", "Unicom Property")


        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "terrace" in p_type_string.lower() or "detached" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and " bed" in p_type_string.lower():
        return "room"
    else:
        return None