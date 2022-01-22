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
    name = '247propertyservices_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    thousand_separator = ','
    scale_separator = '.' 
    def start_requests(self):
        formdata = {
            "businessCategoryId": "1",
            "propind": "L",
            "Location": "",
            "BedsEqual": "",
            "MinPrice": "",
            "MaxPrice": "",
            "RentPricelist": "100;150;200;250;300;350;400;450;500;500;600;700;800;900;1000;1000;1250;1500;1750;2000;2000;3000;4000;5000;",
            "SalesPricelist": "50000;75000;100000;125000;150000;175000;200000;225000;250000;275000;300000;325000;350000;375000;400000;425000;450000;475000;500000;500000;550000;600000;650000;700000;750000;800000;850000;900000;950000;1000000;",
            "sortBy": "highestPrice",
            "searchType": "grid",
            "SalesClassificationRefId": "",
            "featureIds": "",
            "hideProps": "1",
            "roomsOnly": "",
            "PropInd": "L",
        }
        url = "https://www.247propertyservices.co.uk/properties/"
        yield FormRequest(
            url,
            callback=self.parse,
            formdata=formdata,
        )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='photo-cropped']/a"):
            status = item.xpath(".//div[@class='status']/img/@alt").get()
            if status and "agreed" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./@href").get()).split("?")[0].replace("/properties", "")
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.247propertyservices.co.uk/properties/?page={page}&pageSize=12&orderBy=PriceSearchAmount&orderDirection=DESC&propInd=L,%20L&businessCategoryId=1&searchType=grid&hideProps=1&stateValues=1&stateValues=2"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1}
            )
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url.split("?")[0])

        f_text = " ".join(response.xpath("//div[@class='bedswithtype']//text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            f_text = " ".join(response.xpath("//div[contains(@class,'col-lg-5 rightcolumn')]//text()").getall())
            if get_p_type_string(f_text):
                item_loader.add_value("property_type", get_p_type_string(f_text))
            else:
                return
        item_loader.add_value("external_source", "247propertyservices_Co_PySpider_united_kingdom")     
   
        title = " ".join(response.xpath("//div[@class='bedswithtype']//text()").extract())
        if title:
            title = re.sub("\s{2,}", " ", title) 
            item_loader.add_value("title",title.strip()) 
        address = response.xpath("//div[@class='address']/text()").extract_first()
        if address:
            item_loader.add_value("address",address.strip())               
            city = address.split(",")[-1].split(" - ")[0].strip()
            if city.isalpha():
                item_loader.add_value("city",city)   
            else:
                if len(address.split(","))>2:
                    city = address.split(",")[-2].strip()
                    zipcode = address.split(",")[-1].strip()
                    if " Road" not in city:
                        item_loader.add_value("city",city)  
                    if zipcode and "Studio " not in zipcode:
                        item_loader.add_value("zipcode",zipcode)  

        rent = " ".join(response.xpath("//div[@class='price']//text()").extract())
        if rent:
            if "pw" in rent:
                rent = rent.split('£')[-1].strip().split('pw')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)) * 4))
                item_loader.add_value("currency", 'GBP')
            else:
                item_loader.add_value("rent_string",rent ) 
        item_loader.add_xpath("room_count","//span[@class='beds']/text()" ) 
        item_loader.add_xpath("bathroom_count","//span[@class='bathrooms']/text()" ) 

        terrace = response.xpath("//div[@class='features']//li/text()[contains(.,'Terrace')]").extract_first()
        if terrace:
            item_loader.add_value("terrace", True)
      
        furnished = response.xpath("//div[@class='features']//li/text()[contains(.,'Furnished') or contains(.,'furnished')]").extract_first()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
        external_id  = response.xpath("substring-after(//div[@class='reference']//text(),':')").extract_first()
        if external_id :
            item_loader.add_value("external_id", external_id.strip())
        pets_allowed  = response.xpath("//div[@class='restrictions']//li/text()[contains(.,'Pets')]").extract_first()
        if pets_allowed :
            if "no" in pets_allowed.lower():
                item_loader.add_value("pets_allowed", False)
            else:
                item_loader.add_value("pets_allowed", True)
          
        parking = response.xpath("//div[@class='restrictions']//li/text()[contains(.,'Parking')] | //div[@class='features']//li/text()[contains(.,'Parking') or contains(.,'Garage')]").extract_first()
        if parking:
            item_loader.add_value("parking",True)                
        desc = " ".join(response.xpath("//div[@class='description']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
    
        script_map = response.xpath("//div[@id='maplinkwrap']/a/@href[contains(.,'lng=')]").get()
        if script_map:
            item_loader.add_value("latitude", script_map.split("lat=")[1].split("&")[0].strip())
            item_loader.add_value("longitude", script_map.split("lng=")[1].split("&")[0].strip())

        images = [response.urljoin(x) for x in response.xpath("//div[@class='propertyimagelist']/div/img[@class='propertyimage']/@src").extract()]
        if images:
            item_loader.add_value("images", images)
    
        item_loader.add_value("landlord_name", "247 Property Services")
        item_loader.add_value("landlord_phone", "033 00 88 67 48")
        item_loader.add_value("landlord_email", "info@247propertyservices.co.uk")

        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    elif p_type_string and "en-suite" in p_type_string.lower():
        return "room"
    else:
        return None