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
    name = 'churchillmathesons_com'
    execution_type = "testing"
    country = "united_kingdom"
    locale = "en"
    def start_requests(self):
        formdata = {
            "sortorder": "price-desc",
            "RPP": "999999999",
            "OrganisationId": "32af74b4-e9b4-456c-84c4-bbeac68a84b2",
            "WebdadiSubTypeName": "Rentals",
            "Status": "{2a50fde6-8f09-4d01-9514-7a856e206d04},{e9617465-c405-4b6a-abc9-fdbfc499145c},{59c95297-2dca-4b55-9c10-220a8d1a5bed}",
            "includeSoldButton": "false",
            "incsold": "false",
        }
        url = "http://www.churchillmathesons.com/api/set/results/list"
        yield FormRequest(
            url,
            callback=self.parse,
            formdata=formdata,
        )

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='carousel-wrap']/div/@data-url").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)

        ext_id = response.url.split("property/")[1].split("/")[0].strip()
        if ext_id:
            item_loader.add_value("external_id", ext_id)
        
        rented = response.xpath("//section[@id='description']//h2//text()[contains(.,'Let Agreed')]").extract_first()
        if rented:   
            return
        f_text = " ".join(response.xpath("//section[@id='description']//h2/text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            f_text = " ".join(response.xpath("//section[@id='description']//text()").getall())
            if get_p_type_string(f_text):
                item_loader.add_value("property_type", get_p_type_string(f_text))
            else:
                return
        room_count = response.xpath("//section[contains(@class,'featured-stats')]//div//ul[@class='FeaturedProperty__list-stats']/li[img[contains(@src,'bedroom')]]/span/text()[.!='0']").extract_first()
        if room_count:   
            item_loader.add_xpath("room_count",room_count)
        elif "studio" in get_p_type_string(f_text):
            item_loader.add_xpath("room_count","1")
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Churchillmathesons_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/span/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h1/span[@class='displayPostCode']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1/span[@class='city']/text()", input_type="F_XPATH",replace_list={",":""} )
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/span/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//section[@id='description']/p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//section[contains(@class,'featured-stats')]//div//ul[@class='FeaturedProperty__list-stats']/li[img[contains(@src,'bathroom')]]/span/text()[.!='0']", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[contains(@class,'property-price')]//span[@class='nativecurrencyvalue']/text()", input_type="F_XPATH", get_num=True, split_list={".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//div[@id='collapseOne']//li[contains(.,'AVAILABLE')]/text()", input_type="F_XPATH",split_list={'AVAILABLE':1})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='propertyDetailsGallery']/div[@class='item']/div/@data-bg", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//section[@id='maps']/@data-cords", input_type="F_XPATH",split_list={'"lat": "':1,'"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//section[@id='maps']/@data-cords", input_type="F_XPATH",split_list={'"lng": "':1,'"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//div[@id='collapseOne']//li[contains(.,'EPC Rating')]/text()", input_type="F_XPATH",split_list={'EPC Rating':1})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//div[@id='collapseOne']//li[contains(.,'Parking')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//div[@id='collapseOne']//li[contains(.,'Lift')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//div[@id='collapseOne']//li[contains(.,'UNFURNISHED')]/text()", input_type="F_XPATH", tf_item=True, tf_value=False)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//div[@id='collapseOne']//li[contains(.,'FURNISHED')]/text()", input_type="F_XPATH", tf_item=True, tf_value=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//div[contains(@class,'office-details bg-secondary')]/text()[2]", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[contains(@class,'office-details bg-secondary')]/a[contains(@href,'tel')]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="//div[contains(@class,'office-details bg-secondary')]/a[contains(@href,'mailto')]/text()", input_type="F_XPATH")
                
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