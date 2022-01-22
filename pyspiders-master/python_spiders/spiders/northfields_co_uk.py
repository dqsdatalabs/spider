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
    name = 'northfields_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.' 
    external_source = "Northfields_Co_PySpider_united_kingdom"
    def start_requests(self):
        formdata = {
            "action": "apf_property_search",
            "apf_security": "cdb05aa33d",
            "search_data[apf_market]": "residential",
            "search_data[apf_dept]": "to-let",
            "search_data[apf_location]": "",
            "search_data[apf_minprice]": "",
            "search_data[apf_maxprice]": "",
            "search_data[apf_minbeds]": "0",
            "search_data[apf_maxbeds]": "100",
            "search_data[apf_view]": "grid",
            "search_data[apf_map]": "map",
            "search_data[apf_status]": "exclude",
            "search_data[apf_branch]": "",
            "search_data[apf_order]": "price_desc",
        }
        url = "https://www.northfields.co.uk/wp-admin/admin-ajax.php"
        yield FormRequest(
            url,
            callback=self.parse,
            formdata=formdata,
        )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='apf__property__border']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            formdata = {
                "action": "apf_property_search",
                "apf_security": "cdb05aa33d",
                "search_data[apf_market]": "residential",
                "search_data[apf_dept]": "to-let",
                "search_data[apf_location]": "",
                "search_data[apf_minprice]": "",
                "search_data[apf_maxprice]": "",
                "search_data[apf_minbeds]": "0",
                "search_data[apf_maxbeds]": "100",
                "search_data[apf_view]": "grid",
                "search_data[apf_map]": "map",
                "search_data[apf_status]": "exclude",
                "search_data[apf_branch]": "",
                "search_data[apf_order]": "price_desc",
                "search_data[apf_page]": str(page),
            }
            url = "https://www.northfields.co.uk/wp-admin/admin-ajax.php"
            yield FormRequest(
                url,
                callback=self.parse,
                formdata=formdata,
                meta={"page":page+1}
            )
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("-")[-1].split("/")[0])

        f_text = " ".join(response.xpath("//h1/span/text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            f_text = " ".join(response.xpath("//h2[contains(.,'About')]/..//text()").getall())
            if get_p_type_string(f_text):
                item_loader.add_value("property_type", get_p_type_string(f_text))
            else:
                return
        
        item_loader.add_value("external_source", self.external_source)

        item_loader.add_xpath("title", "//h1/span/text()")      
        city = response.xpath("//title/text()").extract_first()
        if city:  
            item_loader.add_value("city", city.split("- ")[-1].strip())
        address = response.xpath("//h1/text()").extract_first()
        if address:
            item_loader.add_value("address", address)
            zipcode = address.split(",")[-1]
            if not zipcode.replace(" ","").isalpha():
                item_loader.add_value("zipcode", zipcode.strip())

        rent = " ".join(response.xpath("//div[@class='digits']//text()").extract())
        if rent:
             item_loader.add_value("rent_string",rent )    
        available_date = " ".join(response.xpath("//article[@class='apf__single__property__features']/ul/li[contains(.,'Available ')]//text()").extract())
        if available_date:  
            available_date = available_date.split('Available')[1].strip()
            date_parsed = dateparser.parse(available_date.replace("Immediately","now"), date_formats=["%d-%m-%Y"], languages=['en'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        room_count =response.xpath("//li[span[@class='fi flaticon-bed']]//text()").extract_first()    
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        elif "studio" in get_p_type_string(f_text):
            item_loader.add_value("room_count", "1")

        item_loader.add_xpath("bathroom_count","//li[span[@class='fi flaticon-bath']]//text()")
        floor =response.xpath("//article[@class='apf__single__property__features']/ul/li[contains(.,'floor') and not(contains(.,'area')) and not(contains(.,'flooring'))]//text()").extract_first()    
        if floor:
            item_loader.add_value("floor", floor.split("floor")[0].strip())
        square =response.xpath("//article[@class='apf__single__property__features']/ul/li[contains(.,'floor are') or contains(.,'Floor Are') or contains(.,'Floor are') or contains(.,'floor space') or contains(.,'floorm space')]//text()").extract_first()    
        sq=""
        if square:       
            unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(m²|meters2|metres2|meter2|metre2|mt2|m2|M2|sq)",square.replace(",","."))
            if unit_pattern:
                sq=int(float(unit_pattern[0][0]))
                item_loader.add_value("square_meters", str(sq))
        else:
            sq = response.xpath("//article[@class='apf__single__property__features']/ul/li[contains(.,'Total area') or contains(.,'Total size')]//text()").get()
            if sq:
                sq =sq.split(":")[1].strip().split(" ")[0].strip()
                item_loader.add_value("square_meters", sq)
        
        if not sq:
            sq = response.xpath("//article[@class='apf__single__property__features']/ul/li[contains(.,'sq')]//text()").get()
            if sq:
                sq = sq.strip().split(" ")[0]
                if sq.isdigit():
                    item_loader.add_value("square_meters", int(float(sq)))

        furnished =response.xpath("//article[@class='apf__single__property__features']/ul/li[contains(.,'furnished') or contains(.,'Furnished')]//text()").extract_first()    
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
        balcony =response.xpath("//article[@class='apf__single__property__features']/ul/li[contains(.,'Balcony') or contains(.,'balcony')]//text()").extract_first()    
        if balcony:
            item_loader.add_value("balcony", True)

        dishwasher  =response.xpath("//article[@class='apf__single__property__features']/ul/li[contains(.,'Dishwasher')]//text()").extract_first()    
        if dishwasher :
            item_loader.add_value("dishwasher", True)
        energy_label =response.xpath("//article[@class='apf__single__property__features']/ul/li[contains(.,'EPC')]//text()").extract_first()    
        if energy_label:
            energy_label = energy_label.split(" ")[-1].strip()
            if energy_label in ["A","B","C","D","E","F","G"]:
                item_loader.add_value("energy_label",energy_label)
        images = [x for x in response.xpath("//div[@class='property__gallery']/div/@data-src").extract()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_xpath("latitude", "//div[@id='map_single']/@data-lat")
        item_loader.add_xpath("longitude","//div[@id='map_single']/@data-lng")
            
        parking =response.xpath("//article[@class='apf__single__property__features']/ul/li[contains(.,'Parking') or contains(.,'parking')]//text()").extract_first()    
        if parking:
            item_loader.add_value("parking", True)
    
        desc = " ".join(response.xpath("//h2[.='About this property']/../p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        landlord_name =response.xpath("//div[@class='branch__details']/div[@class='branch__dept']/h4/text()").extract_first()    
        if landlord_name:
            item_loader.add_value("landlord_name",landlord_name.split())
            item_loader.add_xpath("landlord_phone", "//div[@class='branch__details']//p[i[@class='fal fa-phone']]//text()")
            item_loader.add_xpath("landlord_email", "//input[@name='input_29']/@value")   
        else:
            item_loader.add_value("landlord_name","Northfield Avenue")
            item_loader.add_value("landlord_phone", "020 8567 6660")
      
        floor_urls= response.url + "floorplan/"
        yield Request(floor_urls,callback=self.parse_floor, meta={"item_loader": item_loader})

    def parse_floor(self, response):
        item_loader= response.meta.get("item_loader")
        floor_plan_images = [x for x in response.xpath("//article/h2[.='Floorplans']/../ul/li//img/@src").extract()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

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
    else:
        return None