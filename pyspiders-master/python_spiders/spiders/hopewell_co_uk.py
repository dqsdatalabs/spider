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
    name = 'hopewell_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://hopewell.co.uk/view-all-properties/?department=residential-lettings&marketing_flag=76&minimum_rent=&maximum_rent=&minimum_price=&maximum_price=&minimum_bedrooms=",
                ],
                "property_type": "apartment"
            },
            {
                "url": [
                    "https://hopewell.co.uk/view-all-properties/?department=residential-lettings&marketing_flag=78&minimum_rent=&maximum_rent=&minimum_price=&maximum_price=&minimum_bedrooms="
                ],
                "property_type": "studio"
            },
	        {
                "url": [
                    "https://hopewell.co.uk/view-all-properties/?department=residential-lettings&marketing_flag=75&minimum_rent=&maximum_rent=&minimum_price=&maximum_price=&minimum_bedrooms="
                ],
                "property_type": "student_apartment"
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='details']"):
            follow_url = response.urljoin(item.xpath("./h6//@href").get())
            status = item.xpath(".//div[@class='availability']/p/text()[contains(.,'Let Agreed')]").get()
            if not status:
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        
        next_page = response.xpath("//a[contains(@class,'next page-number')]/@href").get()
        if next_page:
            yield Request(next_page, callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Hopewell_Co_PySpider_united_kingdom")

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//p[@class='property-address']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//p[@class='property-address']/text()", input_type="F_XPATH", split_list={",":-2})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//p[@class='property-address']/text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='price']/text()", input_type="F_XPATH", get_num=True, split_list={" ":0}, replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        
        if response.xpath("//h1/text()[contains(.,'Studio')]").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="1", input_type="VALUE", get_num=True)
        elif response.xpath("//div[contains(@class,'bedroom')]/p/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[contains(@class,'bedroom')]/p/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[contains(@class,'bathroom')]/p/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        energy_label = response.xpath("//li[@class='action-epc']//@href[contains(.,'epc_ce')]").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("epc_ce")[1].split("_")[0])
        
        import dateparser
        available = "".join(response.xpath("//div[@class='description-contents']//p//text()").getall())
        if "available" in available.lower():
            available = available.lower().split("available")[1].replace(",",".").split(".")[0].replace("from the","").strip()
            date_parsed = dateparser.parse(available, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="substring-after(//link[@rel='shortlink']/@href,'=')", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,' furnished') or contains(.,'Furnished')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[contains(.,'Balcon') or contains(.,'balcon')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'Parking') or contains(.,'parking') or contains(.,'Garage') or contains(.,'garage')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[contains(.,'Lift') or contains(.,'lift')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li[contains(.,'Terrace') or contains(.,'terrace')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='description-contents']//p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'LatLng(')]/text()", input_type="F_XPATH", split_list={"LatLng(":1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'LatLng(')]/text()", input_type="F_XPATH", split_list={"LatLng(":1,",":1,")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[@class='slides']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="HOPEWELL", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="0117 911 8663", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="hello@hopewell.co.uk", input_type="VALUE")
        
        yield item_loader.load_item()