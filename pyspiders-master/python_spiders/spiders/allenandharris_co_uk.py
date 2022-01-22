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
    name = 'allenandharris_co_uk'
    execution_type = 'testing' 
    country = 'united_kingdom'
    locale = 'en'
    def start_requests(self):
        formdata = {
            "SearchParameters.SaleType": "Let",
            "SearchParameters.Location": "London, City of London",
            "SearchParameters.UrlToken": "city-of-london",
            "SearchParameters.SearchRadius": "0",
            "SearchParameters.SSTC": "False",
            "SearchParameters.Premium": "Include",
            "page": "1",
            "sort": "Price",
            "sortDirection": "Descending",
            "searchResultsViewMode": "List",
            "itemsPerPage": "12",
            "X-Requested-With": "XMLHttpRequest",
        }
        url = "https://www.allenandharris.co.uk/property/results"
        yield FormRequest(
            url,
            callback=self.parse,
            formdata=formdata,
        )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//a[@class='property-image']"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            formdata = {
                "SearchParameters.SaleType": "Let",
                "SearchParameters.Location": "London, City of London",
                "SearchParameters.UrlToken": "city-of-london",
                "SearchParameters.SearchRadius": "0",
                "SearchParameters.SSTC": "False",
                "SearchParameters.Premium": "Include",
                "page": str(page),
                "sort": "Price",
                "sortDirection": "Descending",
                "searchResultsViewMode": "List",
                "itemsPerPage": "12",
                "X-Requested-With": "XMLHttpRequest",
            }
            url = "https://www.allenandharris.co.uk/property/results"
            yield FormRequest(
                url,
                callback=self.parse,
                formdata=formdata,
                meta={
                    "page":page+1
                }
            )
         
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        f_text = " ".join(response.xpath("//p[@itemprop='description']//text()").getall())
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)
        else:
            f_text = " ".join(response.xpath("//ul[contains(@class,'key-features')]//text()").getall())
            if get_p_type_string(f_text):
                prop_type = get_p_type_string(f_text)

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else: return   

        title=response.xpath("//h2[@class='text-light text-capitalize']/text()").get()
        item_loader.add_value("title",title)

        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Allenandharris_Co_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="rent_string", input_value="//div[contains(@class,'price-container')][1]/span/text()", input_type="F_XPATH",replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div[contains(@class,'property-type')][1]/span/span/text()", input_type="F_XPATH")
        # ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[contains(@class,'property-address')]/small/strong/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[@title='Bathrooms']/span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//div[@title='EPC Rating']/span/text()", input_type="F_XPATH")
        address = response.xpath("//title/text()").get()
        if address:
            address = address.split(" - ")[0].strip()
            item_loader.add_value("address", address)
            if address.count(",") > 0:
                zipcode = address.split(",")[-1].strip()
                if zipcode.replace(" ","").isalpha():
                    item_loader.add_value("city", address.split(",")[-1].strip())
                else:
                    item_loader.add_value("zipcode", address.split(",")[-1].strip())
                    item_loader.add_value("city", address.split(",")[-2].strip())

        latlng = "".join(response.xpath("//script/text()[contains(.,'googleMapControl')]").extract())
        if latlng:
            lat = latlng.split('"addMarker",')[1].split(",")[0].strip()
            lng = latlng.split('"addMarker",')[1].split(",")[1].split(",")[0].strip()
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)
 

        # square_meters = " ".join(response.xpath("//ul[contains(@class,'key-features')]/li/text()[contains(.,'Sqft')]").getall())
        # if square_meters:
        #     if "Sqm" in square_meters:
        #         item_loader.add_value("square_meters", square_meters.split("Sqm")[0].strip())
        #     else:
        #         unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(Sqft.|sq. ft.|Sq. ft.|sq. Ft.|sq|Sq)",square_meters.replace(",",""))
        #         if unit_pattern:
        #             square_title=unit_pattern[0][0]
        #             sqm = str(int(float(square_title) * 0.09290304))
        #             item_loader.add_value("square_meters", sqm)

        square_meters = " ".join(response.xpath("//ul[contains(@class,'key-features')]/li/text()[contains(.,'Sqft')]").getall())
        if square_meters:
            square_meters=re.findall("\d+",square_meters)
            item_loader.add_value("square_meters",square_meters)

        floor = " ".join(response.xpath("//ul[contains(@class,'key-features')]/li/text()[contains(.,'Floor')]").getall())
        if floor:
            item_loader.add_value("floor", floor.split("Floor")[0].strip())

        room = response.xpath("//div[@title='Bedrooms']/span/text()").extract_first()
        if room:
            item_loader.add_value("room_count", room.strip()) 
        else:
            item_loader.add_xpath("room_count", "//div[@title='Reception Rooms']/span/text()")

        bathroom = response.xpath("//ul[@class='clear key-features']/li[contains(.,'Bathroom')]/text()").extract_first()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom.strip()) 


        # address = " ".join(response.xpath("//div[contains(@class,'property-address')]//text()").getall())
        # if address:
        #     item_loader.add_value("address", re.sub("\s{2,}", " ", address))
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//p[contains(@class,'property-description')]/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//ul[@class='clear key-features']//li[contains(.,'Garage') or contains(.,'Parking')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//ul[@class='clear key-features']//li[contains(.,'Balcony') or contains(.,'balcony')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='carousel-inner']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Barnard Marcus", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="0208 878 3540", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="eastsheenlet@barnardmarcus.co.uk", input_type="VALUE")
        pool = " ".join(response.xpath("//ul[contains(@class,'key-features')]/li/text()[contains(.,'Resident Only')]").extract())
        if pool:
            if "pool" in pool:
                item_loader.add_value("swimming_pool",True)
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "terrace" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "detached" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None