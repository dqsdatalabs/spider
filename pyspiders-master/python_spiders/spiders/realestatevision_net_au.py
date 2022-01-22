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
    name = 'realestatevision_net_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url": "https://www.realestatevision.net.au/propertiesJson?callback=angular.callbacks._0&currentPage=1&perPage=24&sort=d_listing%20desc&listing_cat=rental&category_ids=44",
                "property_type": "house",
            },
	        {
                "url": "https://www.realestatevision.net.au/propertiesJson?callback=angular.callbacks._0&currentPage=1&perPage=24&sort=d_listing%20desc&listing_cat=rental&category_ids=45",
                "property_type": "apartment",
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        seen = False
        jsep = str(response.body).split("(")[1].split(");")[0]
        try:
            data = json.loads(jsep)
            for item in data["rows"]:
                url = item["fields"]["link"].replace("\/","/")
                yield Request(response.urljoin(url),
                            callback=self.populate_item, 
                            meta={"property_type":response.meta.get('property_type')})
                seen = True
        except: pass
        
        if page == 1 or seen:
            url = response.url.replace(f"Page={page-1}", f"Page={page}")
            yield Request(
                url,
                callback=self.parse, 
                meta={"page": page+1, "property_type":response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Realestatevision_Net_PySpider_australia", input_type="VALUE")
        if response.xpath("//tr/th[contains(.,'Suburb')]/following-sibling::td/text()"):
            ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//tr/th[contains(.,'Suburb')]/following-sibling::td/text()", input_type="F_XPATH")
            ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//tr/th[contains(.,'Suburb')]/following-sibling::td/text()", input_type="F_XPATH")
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//tr/th[contains(.,'Address')]/following-sibling::td/text()", input_type="F_XPATH")
            ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//tr/th[contains(.,'Address')]/following-sibling::td/text()", input_type="F_XPATH", split_list={",":-1})
            
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[contains(@class,'details-section')]//p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//i[contains(@class,'bedroom')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//i[contains(@class,'bathroom')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//tr/th[contains(.,'Price')]/following-sibling::td/text()", input_type="F_XPATH", get_num=True, per_week=True, split_list={" ":0, ".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="AUD", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//tr/th[contains(.,'Available')]/following-sibling::td/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//tr/th[contains(.,'ID')]/following-sibling::td/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'owl-carousel')]//@data-src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'center: [')]/text()", input_type="F_XPATH", split_list={"center: [":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'center: [')]/text()", input_type="F_XPATH", split_list={"center: [":1, ",":1,"]":0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//i[contains(@class,'garage')]/following-sibling::span/text()[.!='0']", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//div[contains(@class,'consultant-list')]//h4/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//i[contains(@class,'contact-phone')]/following-sibling::text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="admin@realestatevision.net.au", input_type="VALUE")

        yield item_loader.load_item()