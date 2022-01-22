# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'charnwoodliving_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    custom_settings = {"HTTPCACHE_ENABLED":False}
    external_source = "Charnwoodliving_Co_PySpider_united_kingdom"
    post_urls = ['https://www.charnwoodliving.co.uk/search.vbhtml?properties-to-rent']  # LEVEL 1
    
    formdata = {
        "salerent": "nr",
        "minprice": "",
        "maxprice": "",
        "minbeds": "",
        "PropPerPage": "12",
        "order": "low",
        "radius": "0",
        "letagreedorstc": "true",
        "grid": "grid",
        "search": "yes",
        "residential":"",
    }
    
    def start_requests(self):
        yield FormRequest(
            url=self.post_urls[0],
            formdata=self.formdata,
            callback=self.parse
        )

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[contains(@class,'info-image')]/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            status = item.xpath(".//span[contains(@class,'status')]/@style[contains(.,'letout') or contains(.,'under')]").get()
            if not status:
                yield Request(follow_url, callback=self.populate_item)
            seen = True
         
        if page == 2 or seen:
            self.formdata["links"] = str(page)
            yield FormRequest(self.post_urls[0], dont_filter=True, formdata=self.formdata, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("&")[1].upper())
        description = "".join(response.xpath("//p[@class='lead']//text()").getall())
        if get_p_type_string(description):
            item_loader.add_value("property_type", get_p_type_string(description))
        # else: return
        item_loader.add_value("external_source", "Charnwoodliving_Co_PySpider_united_kingdom")

        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title)
            item_loader.add_value("zipcode", title.split(",")[-1].split("-")[0].strip())

        address = response.xpath("//h1[contains(@class,'page-header')]/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[-1].strip())

        rent = response.xpath("//span[contains(@class,'fullprice')]/text()").get()
        if rent:
            item_loader.add_value("rent", rent.replace(",","").replace("£",""))
        item_loader.add_value("currency", "GBP")

        features = response.xpath("//p[@class='photos-pad']/text()[1]").get()
        if "bedroom" in features.lower():
            room_count = features.lower().split("bedroom")[0].strip()
            item_loader.add_value("room_count", room_count)
        if "bathroom" in features.lower():
            bathroom_count = features.lower().split("bathroom")[0].strip().split(" ")[-1]
            item_loader.add_value("bathroom_count", bathroom_count)
        
        floor = response.xpath("//li[contains(.,'Floor')]/text()").get()
        if floor:
            floor = floor.split("Floor")[0].strip().split(" ")[-1]
            item_loader.add_value("floor", floor)
        
        if description:
            item_loader.add_value("description", description.strip())
            
        images = [response.urljoin(x) for x in response.xpath("//section[contains(@class,'propertyfullpage')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@id='floorplan']//@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        energy_label = response.xpath("//div[@id='EPC']//@src[contains(.,'epc1')]").get()
        if energy_label:
            energy_label = energy_label.split("epc1=")[1].split("&")[0]
            if energy_label !='0': item_loader.add_value("energy_label", energy_label)
        
        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng')[1].split(',')[1].split(')')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        item_loader.add_value("landlord_name", "Charnwood Living")
        item_loader.add_value("landlord_phone", "0116 243 0880")

        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "mid terrace" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return "apartment"