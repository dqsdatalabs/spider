# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear
import dateparser
import re

class MySpider(Spider):
    name = 'haus_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    headers = {
        'authority': 'haus.com.au',
        'content-length': '0',
        'accept': '*/*',
        'origin': 'https://haus.com.au',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'referer': 'https://haus.com.au/listings/?post_type=listings&count=20&orderby=meta_value&meta_key=dateListed&sold=0&saleOrRental=Rental&order=dateListed-desc&paged=1&extended=1&minprice=&maxprice=&minbeds=&maxbeds=&baths=&cars=&type=residential&externalID=&subcategory=&landsize=',
        'accept-language': 'tr,en;q=0.9',
        'cookie': '_ga=GA1.3.1629568944.1612325015; _gid=GA1.3.1325748217.1612325015; _gcl_au=1.1.183033693.1612325027; _fbp=fb.2.1612325027232.656383152; _gat=1'
    }

    def start_requests(self):
        start_url = "https://haus.com.au/listings/?post_type=listings&count=20&orderby=meta_value&meta_key=dateListed&sold=0&saleOrRental=Rental&order=dateListed-desc&paged=1&extended=1&type=residential"
        yield FormRequest(start_url, headers=self.headers, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False
        data = response.xpath("//script[contains(.,'MapDataStore ')]/text()").get()
        data = data.split("MapDataStore =")[1].split(";")[0].strip()
        data = json.loads(data)
        for item in data:
            seen = True
            follow_url = item["url"]
            yield Request(follow_url, callback=self.populate_item, meta={"item":item})

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get("property_type"))
        item_loader.add_value("external_link", response.url)
        title=response.xpath("//p[@class='single-listing-address ']/text()").get()
        if title:
            item_loader.add_value("title",title)
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Haus_Com_PySpider_australia", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//p[contains(@class,'address')]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//p[contains(@class,'address')]/text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//section/div[@class='property-info-bar']/div/p/text()", input_type="F_XPATH", get_num=True, replace_list={"$":""})
        zipcode=response.xpath("//script[contains(.,'postAddress')]/text()").get()
        if zipcode:
            zipcode=zipcode.split("postAddress")[-1].split(";")[0].split(",")[-1]
            zipcode=re.findall("\d+",zipcode)
            item_loader.add_value("zipcode",zipcode)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="AUD", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//p[contains(@class,'bed')]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//p[contains(@class,'bath')]/text()", input_type="F_XPATH", get_num=True)
        
        description = "".join(response.xpath("//div[@class='b-description__text']//text()").getall())
        if get_p_type_string(description):
            item_loader.add_value("property_type", get_p_type_string(description))
        else:
            p_text = response.xpath("//h5[contains(@class,'post-title')]/text()").get()
            if get_p_type_string(p_text):
                item_loader.add_value("property_type", get_p_type_string(p_text))
            else:
                print(response.url)
                return

        item_loader.add_value("description", description)
        
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div[@class='section-header'][contains(.,'Key')]//strong[contains(.,'ID')]/following-sibling::text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//div[@class='section-header'][contains(.,'Key')]//strong[contains(.,'price')]/following-sibling::text()", input_type="F_XPATH", split_list={"$":1}, replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//div[@class='section-header'][contains(.,'Key')]//strong[contains(.,'available')]/text()", input_type="F_XPATH", split_list={"available":1})
        available_datecheck=item_loader.get_output_value("available_date")
        if not available_datecheck:
            availabledate=response.xpath("//strong[contains(.,'date available')]/following-sibling::text()").get()
            if availabledate:
                date_parsed = dateparser.parse(availabledate, date_formats=["%m-%d-%Y"])
                date2 = date_parsed.strftime("%Y-%m-%d")
                if date2:
                    item_loader.add_value("available_date", date2)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//p[contains(@class,'car')]/text()[.!='0']", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='media-gallery']//@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={"lat:":1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={"lng:":1,"}":0})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//h5[contains(@class,'card-title')]/a/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//span[contains(@class,'phone-number')]/a/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="//p/a[contains(@href,'mail')]/text()[contains(.,'@')]", input_type="F_XPATH")
        
        yield item_loader.load_item() 

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower()):
        return "house"
    elif "bedroom" in p_type_string:
        return "apartment"
    else:
        return None