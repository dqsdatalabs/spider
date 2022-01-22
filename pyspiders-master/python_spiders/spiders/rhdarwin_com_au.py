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
    name = 'rhdarwin_com_au'
    execution_type='testing'
    country='australia' 
    locale='en'
    external_source = 'Rhdarwin_Com_PySpider_australia'
    custom_settings = {
        "PROXY_TR_ON": True,   
        "HTTPCACHE_ENABLED": False,
        "HTTPERROR_ALLOWED_CODES": [301,302,403]
    }  
    def start_requests(self):
        headers = {
            'authority': 'www.raineandhorne.com.au',
            'accept': '*/*;q=0.5, text/javascript, application/javascript, application/ecmascript, application/x-ecmascript',
            'x-csrf-token': '2AwfrA8M0vxf06x+Jv71M02DWuZ5nyOy2bnp3vUCAMwI9hpZoj72RvC7YGW+BXJdOXaEP7sYtEMicHAOPKx8IA==',
            'x-requested-with': 'XMLHttpRequest',
            'referer': 'https://www.raineandhorne.com.au/search/properties?listing_type=residential&offer_type_code=rental&page=2&per_page=12&property_type=Studio&status=active&surrounding_suburbs=0',
            'accept-language': 'tr,en;q=0.9',
        }
        property_types = [ 
                {
                    "url": [
                        "https://www.raineandhorne.com.au/search/properties.json?format=json&amp;listing_type=residential&amp;offer_type_code=rental&amp;page=1&amp;per_page=5000&amp;property_type=Apartment&amp;status=active&amp;surrounding_suburbs=0",
                        "https://www.raineandhorne.com.au/search/properties.json?format=json&amp;listing_type=residential&amp;offer_type_code=rental&amp;page=1&amp;per_page=5000&amp;property_type=Flat&amp;status=active&amp;surrounding_suburbs=0",
                        "https://www.raineandhorne.com.au/search/properties.json?format=json&amp;listing_type=residential&amp;offer_type_code=rental&amp;page=1&amp;per_page=5000&amp;property_type=Unit&amp;status=active&amp;surrounding_suburbs=0",
                    ],
                    "property_type": "apartment",
                },
                {
                    "url": [
                        "https://www.raineandhorne.com.au/search/properties.json?format=json&amp;listing_type=residential&amp;offer_type_code=rental&amp;page=1&amp;per_page=5000&amp;property_type=DuplexSemi-detached&amp;status=active&amp;surrounding_suburbs=0",
                        "https://www.raineandhorne.com.au/search/properties.json?format=json&amp;listing_type=residential&amp;offer_type_code=rental&amp;page=1&amp;per_page=5000&amp;property_type=House&amp;status=active&amp;surrounding_suburbs=0",
                        "https://www.raineandhorne.com.au/search/properties.json?format=json&amp;listing_type=residential&amp;offer_type_code=rental&amp;page=1&amp;per_page=5000&amp;property_type=Terrace&amp;status=active&amp;surrounding_suburbs=0",
                        "https://www.raineandhorne.com.au/search/properties.json?format=json&amp;listing_type=residential&amp;offer_type_code=rental&amp;page=1&amp;per_page=5000&amp;property_type=Townhouse&amp;status=active&amp;surrounding_suburbs=0",
                        "https://www.raineandhorne.com.au/search/properties.json?format=json&amp;listing_type=residential&amp;offer_type_code=rental&amp;page=1&amp;per_page=5000&amp;property_type=Villa&amp;status=active&amp;surrounding_suburbs=0",
                    ],
                    "property_type": "house",
                },
                {
                    "url": [
                        "https://www.raineandhorne.com.au/search/properties.json?format=json&amp;listing_type=residential&amp;offer_type_code=rental&amp;page=1&amp;per_page=5000&amp;property_type=Studio&amp;status=active&amp;surrounding_suburbs=0",
                    ],
                    "property_type": "studio",
                }
            ]
        for item in property_types:
            for url in item["url"]:
                yield Request(
                    url=url,
                    headers=headers,
                    callback=self.parse,
                    meta={"property_type": item["property_type"]}
                )

    # 1. FOLLOWING
    def parse(self, response):

        data = json.loads(response.body)
        # print(data)
        for item in data["markers"]:
            if item:
                follow_url = response.urljoin(item[2])
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)     
        dontallow=response.xpath("//h4[@class='details-desc-price']/text()").get()
        if dontallow and "application approved" in dontallow.lower():
            return 
            
        if response.url == "https://www.raineandhorne.com.au" or response.url == "https://www.raineandhorne.com.au/":
            return
        leased = response.xpath("//h2[@class='details-desc-description-title']//text()[contains(.,'LEASED')]").get()
        if leased:           
            return

        description_title = response.xpath("//h4[@class='details-desc-price']/text()").get()
        if description_title:
            description_title = description_title.strip()
            if "Rented out" in description_title:
                return
            if ("From" in description_title) or ("from" in description_title):
                return

        app_approved = response.xpath("//h4[@class='details-desc-price']//text()[contains(.,'Application Approved')]").get()
        if app_approved:
            return
        from python_spiders.helper import ItemClear
           
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//span[@itemprop='addressLocality']//text()", input_type="F_XPATH", replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//span[@itemprop='postalCode']//text()", input_type="F_XPATH")
        
        address = " ".join(response.xpath("//span[@itemprop='address']//text()").getall())
        if address:
            address = re.sub('\s{2,}', ' ', address.strip())
            item_loader.add_value("address", address) 
        
        rent = response.xpath("//h4[contains(@class,'desc-price')]/text()[not(contains(.,'Price on Application'))]").get()
        if rent:
            if "deposit" in rent.lower() or "leased" in rent.lower() or "holding" in rent.lower() or "free" in rent.lower(): return
            
            rent = response.xpath("//h4[contains(@class,'desc-price')]/text()[not(contains(.,'Price on Application'))]").get()
            rent=rent.replace(",","").split("$")[-1].split("/w")[0].split("/W")[0]
            rent=re.findall("\d+",rent)
            if rent:
                item_loader.add_value("rent", int(float(rent[0]))*4)            

        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="AUD", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h2//text()", input_type="F_XPATH")
        
        desc = " ".join(response.xpath("//div[@itemprop='description']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "sqm" in desc:
            square_meters = desc.split("sqm")[0].strip().split(" ")[-1].replace("c.","").replace(",","").replace("(","")
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        not_list = ["whole", "stained"]
        if "floor " in desc:
            floor = desc.split("floor ")[0].strip().split(" ")[-1]
            if "whole" not in floor.lower() and "stained" not in floor.lower():
                item_loader.add_value("floor", floor)
        
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//section[@class='details-desc']//li[@class='beds']/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//section[@class='details-desc']//li[@class='baths']/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'coordinates')]/text()", input_type="F_XPATH", split_list={"coordinates: [":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'coordinates')]/text()", input_type="F_XPATH", split_list={"coordinates: [":1, ",":1, "]":0})
        
        images = [x.split("background-image: url(")[1].split(")")[0] for x in response.xpath("//div[contains(@class,'slider__slide')]//@style").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = response.xpath("normalize-space(//h4[contains(@class,'desc-price')]/text()[contains(.,'Available')])").get()
        if available_date:
            if "now" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date.split(":")[1].strip(), date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
                    
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//section[@class='details-desc']//li[@class='cars']/text()[.!='0']", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//section[@class='details-desc']//li[contains(.,'ID')]/span[2]/text()", input_type="F_XPATH")
        
        if response.xpath("//div[@class='agent-office']/a[1]/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//div[@class='agent-office']/a[1]/text()", input_type="F_XPATH")
        else: item_loader.add_value("landlord_name", "Raine & Horne")
        
        if response.xpath("//button/@data-phone").get():
            ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//button/@data-phone", input_type="F_XPATH")
        else: item_loader.add_value("landlord_phone", "02 9258 5444")
        
        status = response.xpath('//h3[@class="details-desc-sale-type"]/text()').get()
        if status:
            status = status.strip()
            if 'sale' in status.lower():
                return
            else:
                yield item_loader.load_item()