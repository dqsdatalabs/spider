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
    name = 'letsafehomes_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'

    def start_requests(self):
        formdata = {
            "Location": "",
            "BedsEqual": "",
            "MinPrice": "",
            "MaxPrice": "",
            "hideProps": "1",
            "sortBy": "",
            "searchType": "list",
            "searchByMap": "",
            "RentPricelist": "100;150;200;250;300;350;400;450;500;500;600;700;800;900;1000;1000;1250;1500;1750;2000;2000;3000;4000;5000;",
            "SalesPricelist": "100000;125000;150000;175000;200000;225000;250000;275000;300000;325000;350000;375000;400000;425000;450000;475000;500000;500000;550000;600000;650000;700000;750000;800000;850000;900000;950000;1000000;",
            "PropInd": "L",
        }
        url = "https://www.letsafehomes.co.uk/properties.asp"
        yield FormRequest(
            url,
            callback=self.parse,
            formdata=formdata,
        )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//a[@class='detlink']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.letsafehomes.co.uk/properties/?page={page}&propind=L&country=&town=&area=&MinPrice=&MaxPrice=&MinBeds=&BedsEqual=&PropType=&Furn=&Avail=&O=PriceSearchAmount&Dir=ASC&areaId=&lat=&lng=&zoom=&searchbymap=&maplocations=&hideProps=1&location=&searchType=list"
            yield Request(
                p_url,
                callback=self.parse,
                meta={
                    "page":page+1
                }
            )
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url.split('?')[0])
        rented = response.xpath("//div[@class='status']/img/@alt[.='Let Agreed']").get()
        if rented:
            return
        f_text = " ".join(response.xpath("//div[@class='bedswithtype']//text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            f_text = " ".join(response.xpath("//div[@class='description']//text()").getall())
            if get_p_type_string(f_text):
                item_loader.add_value("property_type", get_p_type_string(f_text))
            else:
                return
        address = response.xpath("//div[@class='propertydet']//div[@class='address']/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            if "," in address:
                city = address.split(",")[-1].split(".")[0]
                item_loader.add_value("city", city.strip())
            if ". " in address.strip():
                zipcode =  address.strip().split("*")[0].strip().split(". ")[-1].split(".")[0]
                if "," in zipcode:
                    zipcode = zipcode.split(",")[-1].strip()
                    if " " not in zipcode: zipcode = ""
                
                if zipcode:
                    item_loader.add_value("zipcode", zipcode.strip())
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Letsafehomes_Co_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div[@class='reference']/text()", input_type="F_XPATH", split_list={":":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='description']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[@class='bedsWithTypeBeds']/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//input[@name='mapmarker']/@value[contains(.,\"bathrooms='\")]", input_type="F_XPATH", split_list={"bathrooms='":1, "'":0}, get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='propertyimagelist']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//input[@name='mapmarker']/@value", input_type="F_XPATH", split_list={"lat='":-1, "'":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//input[@name='mapmarker']/@value", input_type="F_XPATH", split_list={"lng='":-1, "'":0})
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//span[@class='bedsWithTypePropType' and contains(.,'terrace')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Letsafe Homes", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="0191 263 9669", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@letsafehomes.co.uk", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//text()[contains(.,'EPC ') or contains(.,'epc ')]", input_type="F_XPATH", split_list={"EPC":1, "RATING":-1, ".":0}, lower_or_upper=1)

        term = response.xpath("//span[@class='displaypricequalifier']/text()").get()
        if term:
            if 'pw' in term.lower(): ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[@class='displayprice']/text()", input_type="F_XPATH", get_num=True, per_week=True)
            elif 'pcm' in term.lower(): ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[@class='displayprice']/text()", input_type="F_XPATH", get_num=True)

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "huis" in p_type_string.lower()):
        return "house"
    else:
        return None