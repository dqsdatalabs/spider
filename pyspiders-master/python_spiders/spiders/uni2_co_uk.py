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
import re

class MySpider(Spider):
    name = 'uni2_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    external_source = "Uni_2_Co_PySpider_united_kingdom"
    start_urls = ["https://www.uni-2.co.uk/rent/"]
    custom_settings = {
        "HTTPCACHE_ENABLED" : False
        }

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        for item in response.xpath("//a[@class='btn btn--block btn--noBorder btn--cover z-30 mr-4 no-animate btn--black']"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//a[@class='next page-numbers']").get()
        if next_page:
            p_url = f"https://www.uni-2.co.uk/rent/page/{page}/"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        # f_text = " ".join(response.xpath("//div[@class='c-main__content']//text()").getall())
        # prop_type = ""
        # if get_p_type_string(f_text):
        #     prop_type = get_p_type_string(f_text)

        # if prop_type:
        #     item_loader.add_value("property_type", prop_type)
        # else: return

        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1/span/text()", input_type="F_XPATH", split_list={",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//div[@id='map']/@data-map", input_type="M_XPATH", split_list={'postcode":"':1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[contains(.,'Bed')]/following-sibling::p[1]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//span[contains(.,'Bath')]/following-sibling::p[1]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//article/p[@class='price']/span/text()", get_num=True, per_week=True, input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//div[@id='map']/@data-map", input_type="M_XPATH", split_list={'lat":"':1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//div[@id='map']/@data-map", input_type="M_XPATH", split_list={'lng":"':1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        
        desc = " ".join(response.xpath("//div[@class='c-main__content']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [x.split("('")[1].split("')")[0] for x in response.xpath("//div[@class='image']/@style").getall()]
        if images:
            item_loader.add_value("images", images)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//div[@class='listing-terms']//text()[contains(.,'FURNISHED')]", input_type="F_XPATH", tf_item=True)
        
        import dateparser
        if "start date is" in desc:
            available_date = desc.split("start date is")[1].split(".")[0].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Uni2 Rent", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="0115 8708069", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="rent@uni-2.co.uk", input_type="VALUE")

        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "terrace" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "detached" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None