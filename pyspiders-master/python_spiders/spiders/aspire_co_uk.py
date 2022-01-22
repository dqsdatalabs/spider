# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from word2number import w2n

class MySpider(Spider):
    name = 'aspire_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {"url": "https://www.aspire.co.uk/search/?country=UK&instruction_type=Letting&price_fuzz_pc=5&address_keyword=&minprice=&bid=&maxprice=&property_type=Apartment", "property_type": "apartment"},
	        {"url": "https://www.aspire.co.uk/search/?country=UK&instruction_type=Letting&price_fuzz_pc=5&address_keyword=&minprice=&bid=&maxprice=&property_type=Flat", "property_type": "apartment"},
            {"url": "https://www.aspire.co.uk/search/?country=UK&instruction_type=Letting&price_fuzz_pc=5&address_keyword=&minprice=&bid=&maxprice=&property_type=New+Flat", "property_type": "apartment"},
            {"url": "https://www.aspire.co.uk/search/?country=UK&instruction_type=Letting&price_fuzz_pc=5&address_keyword=&minprice=&bid=&maxprice=&property_type=House", "property_type": "house"},
            {"url": "https://www.aspire.co.uk/search/?country=UK&instruction_type=Letting&price_fuzz_pc=5&address_keyword=&minprice=&bid=&maxprice=&property_type=Maisonette", "property_type": "house"},
            {"url": "https://www.aspire.co.uk/search/?country=UK&instruction_type=Letting&price_fuzz_pc=5&address_keyword=&minprice=&bid=&maxprice=&property_type=New+House", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")
       
        for item in response.xpath("//strong[contains(.,'VIEW DETAILS')]/../@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})

        next_page = response.xpath("//a[@class='next']/@href").get()
        if next_page:
            follow_url = response.urljoin(next_page)
            yield Request(follow_url, callback=self.parse, meta={"property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Aspire_Co_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value=response.url, input_type="VALUE", split_list={"property-details/":1, "/":0})
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h1/text()", input_type="F_XPATH", split_list={" ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="normalize-space(//title/text())", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='details-description']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//text()[contains(.,'sq ft') or contains(.,'Sq Ft')]", input_type="F_XPATH", split_list={"sq ft":0, " ":-1}, get_num=True, lower_or_upper=0, sq_ft=True)
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//img[contains(@src,'bed-icon')]/following-sibling::text()[1]", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//img[contains(@src,'bath-icon')]/following-sibling::text()[1]", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='details-address']//h2/text()", input_type="F_XPATH", split_list={"£":-1, "p":0}, get_num=True, lower_or_upper=0)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//text()[contains(.,'Available')]", input_type="F_XPATH", split_list={"available":-1}, replace_list={"immediately":"now", "early":"", "end of":"", "from the":""}, lower_or_upper=0)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@id,'smaller-prop-images-0')]//div[@class='carousel-inner']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//h3[contains(.,'Floor Plan')]/following-sibling::img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'ShowMap')]/text()", input_type="F_XPATH", split_list={"&q=":1, "%2C":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'ShowMap')]/text()", input_type="F_XPATH", split_list={"&q=":1, "%2C":1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'Parking') or contains(.,'parking')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[contains(.,'Balcony') or contains(.,'balcony')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Furnished') or contains(.,'furnished')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[contains(.,'Lift') or contains(.,'lift')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li[contains(.,'Terrace') or contains(.,'terrace')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="swimming_pool", input_value="//li[contains(.,'Swimming Pool') or contains(.,'Swimming pool')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Aspire Balham Office", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="020 8673 6111", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="balham@aspire.co.uk", input_type="VALUE")

        yield item_loader.load_item()