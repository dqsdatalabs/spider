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
    name = 'topproperty_services_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["https://topproperty-services.com/propertylist/?area%5B%5D=Any%20Area&show_rooms=All"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[contains(@class,'w3-hide-large w3-hide-small')]//a[@id='hideme_buttonx']"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", "student_apartment")

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Topproperty_Services_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[@style='width:100%;float:left;']/div[position()<3]//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[contains(@style,'align:right;font-size:270%')][1]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//b[contains(.,'Overview')]/../following-sibling::div//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//b[contains(.,'bedroom')]/text()", input_type="F_XPATH", get_num=True, split_list={"bedroom":0})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//b[contains(.,'bathroom')]/text()", input_type="F_XPATH", get_num=True, split_list={"bedroom":0})
        term = response.xpath("//div[contains(@class,'fa-gbp')]/../../../p//text()").get()
        if term:
            if 'PPPW' in term: ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[contains(@class,'fa-gbp')]/../../../p//text()", input_type="F_XPATH", get_num=True, per_week=True)
            elif 'PCM' in term: ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[contains(@class,'fa-gbp')]/../../../p//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//text()[contains(.,'Available from')]", input_type="F_XPATH", replace_list={"Available from":""})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//b[contains(.,'Overview')]/../following-sibling::div//text()[contains(.,'holding deposit')]", input_type="F_XPATH", get_num=True, split_list={"holding deposit":0, ".":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='slider slider-for']/..//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//img[contains(@src,'property.epc')]/@src", input_type="M_XPATH", split_list={"Current=":1, "&":0})
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//div[@style='width:100%;float:left;']/div[last()]//text()", input_type="F_XPATH", tf_item=True, tf_words={True:"Furnished", False:"Unfurnished"})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Topproperty Services", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="0151 733 2200", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@topproperty-services.com", input_type="VALUE")

        yield item_loader.load_item()

