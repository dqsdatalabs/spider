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
import re
 
class MySpider(Spider):  
    name = 'grea_com_au' 
    execution_type='testing'
    country='australia'
    locale='en'
    external_source="Grea_Com_PySpider_australia"
    custom_settings = {
        "HTTPCACHE_ENABLED": False,
    }
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.grea.com.au/rent?searchword8=Flat&searchword10-from=300&searchword10-to=45000&moduleId=107&Itemid=128",
                    "https://www.grea.com.au/rent?searchword8=Apartment&searchword10-from=300&searchword10-to=45000&moduleId=107&Itemid=128",
                    "https://www.grea.com.au/rent?searchword8=Unit&searchword10-from=300&searchword10-to=45000&moduleId=107&Itemid=128"
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.grea.com.au/rent?searchword8=House&searchword10-from=300&searchword10-to=45000&moduleId=107&Itemid=128",
                    "https://www.grea.com.au/rent?searchword8=Townhouse&searchword10-from=300&searchword10-to=45000&moduleId=107&Itemid=128",
                    "https://www.grea.com.au/rent?searchword8=Villa&searchword10-from=300&searchword10-to=45000&moduleId=107&Itemid=128",
                ],
                "property_type" : "house"
            },

        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            dont_filter=True,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='property-item-link']"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("-")[-1])
        from python_spiders.helper import ItemClear
        item_loader.add_value("external_source",self.external_source)
        
        # ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Grea_Com_PySpider_australia", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//script[contains(.,'address')]/text()", input_type="F_XPATH", split_list={'address: "':1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//script[contains(.,'address')]/text()", input_type="F_XPATH", split_list={'address: "':1, '"':0, " ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[@class='beds'][1]/text()[.!='0'][not(contains(.,'0'))]", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//span[@class='beds'][2]/text()[.!='0'][not(contains(.,'0'))]", input_type="F_XPATH", get_num=True)
  
        rent = response.xpath("//h3[@class='propertyPrice']/text()").get()  
        if rent and not "Under Application! "==rent:
            rent = rent.split("-")[-1].replace(",","").strip().split(" ")[0].replace("$","")
            
            if rent.isdigit():
                item_loader.add_value("rent", int(rent)*4)
        # else:
        #     ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//h3[@class='propertyPrice']/text()", input_type="F_XPATH", get_num=True,split_list={" ":0}, per_week=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="AUD", input_type="VALUE")
        
        address = response.xpath("//script[contains(.,'address')]/text()").get()
        if address:
            address = address.split('address: "')[1].split('"')[0].strip().replace("NSW","").split(" ")
            city = ""
            for i in address:
                if i.isupper():
                    city = city+i+" "
            item_loader.add_value("city", city)
        
        desc = " ".join(response.xpath("//div[@class='itemFullText']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        dontallow=item_loader.get_output_value("description")
        if dontallow and "ideal office and workshop" in dontallow.lower():
            return 
        if "sqm" in desc:
            square_meters = desc.split("sqm")[0].strip().split(" ")[-1]
            item_loader.add_value("square_meters", square_meters)
        
        images = [response.urljoin(x.split("'")[1]) for x in response.xpath("//div[@class='itemImageGallery']//@style").getall()]
        if images:
            item_loader.add_value("images", images)

        floor_plan_images = response.xpath("//a[contains(.,'Floorplan')]//@href").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", response.urljoin(floor_plan_images))
        
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//div[@class='itemFullText']//text()[contains(.,'lift')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//div[@class='itemFullText']//text()[contains(.,'terrace')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//div[@class='itemFullText']//text()[contains(.,'balcon') or contains(.,'Balcon')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//span[@class='beds'][3]/text()[.!='0'][not(contains(.,'0'))]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Green Real Estate", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="02 9807 8899", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="office@grea.com.au", input_type="VALUE")

        status = response.xpath("//h3[@class='propertyPrice']/text()").get()
        if "$" in status:
            yield item_loader.load_item()