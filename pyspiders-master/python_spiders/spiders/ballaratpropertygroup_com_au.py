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
    name = 'ballaratpropertygroup_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source='Ballaratpropertygroup_Com_PySpider_australia'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://buxton.com.au/property/?sort_type=date_newest&type=&boundary=&search_type=rent&property_type=Residential+Apartment&bedrooms=&bathrooms=&parking=&rent_min=&rent_max=#page-1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://buxton.com.au/property/?sort_type=date_newest&type=&boundary=&search_type=rent&property_type=Residential+House&bedrooms=&bathrooms=&parking=&rent_min=&rent_max=#page-1",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

       
        url = "https://buxton.com.au/property/results/?sort_type=date_newest&type=&boundary=&search_type=rent&property_type=Residential+House&bedrooms=&bathrooms=&parking=&rent_min=&rent_max=&page=1"
        yield FormRequest(
            url,
            callback = self.parse_listing,
            dont_filter=True,
            meta={"property_type" : response.meta.get("property_type")}
            )
        
    def parse_listing(self,response):
        sel = Selector(text=json.loads(response.body)["results"], type="html")
        for item in sel.xpath("//div[@class='listing-card card']//a[contains(@class,'property-link')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value=self.external_source, input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//link[@rel='shortlink']/@href", input_type="F_XPATH", split_list={"?p=":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/text() | //div[@class='details']/h2/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[@class='details']/h2/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[@class='price']/text()", input_type="F_XPATH", get_num=True, per_week=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="AUD", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[contains(@class,'bedroom')]//div[last()]/div[2]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[contains(@class,'bathroom')]//div[last()]/div[2]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//div[@class='div'][contains(.,'Available')]/following-sibling::div/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//div[contains(@class,'garage')]//div[last()]/div[2]/text()[.!='0']", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//p/strong[contains(.,'Bond')]/following-sibling::text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='description']//p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'LatLng')]/text()", input_type="F_XPATH", split_list={"LatLng(":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'LatLng')]/text()", input_type="F_XPATH", split_list={"LatLng(":1, ",":1, ")":0})

        images = [x for x in response.xpath("//div[@class='image-slider']//ul//li//div[contains(@class,'lazyload image-item')]//@data-src").getall()]
        if images:
            item_loader.add_value("images", images) 

        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="BALLARAT PROPERTY GROUP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="03 53 300 500", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@ballaratpropertygroup.com.au", input_type="VALUE")

        yield item_loader.load_item()