# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
class MySpider(Spider):
    name = 'ella_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    # 1. FOLLOWING
    def start_requests(self):
        start_urls = [
            {
                "property_type" : "apartment",
                "type" : "apartment",
                "url" : "https://www.alexanderandco.co.uk/property-search/apartments-available-to-rent-in-aylesbury-bicester-dunstable-wing-buckingham-winslow"
            },
            {
                "property_type" : "house",
                "type" : "house",
                "url" : "https://www.alexanderandco.co.uk/property-search/houses-available-to-rent-in-aylesbury-bicester-dunstable-wing-buckingham-winslow"
            },
            {
                "property_type" : "apartment",
                "type" : "flat",
                "url" : "https://www.alexanderandco.co.uk/property-search/flats-available-to-rent-in-aylesbury-bicester-dunstable-wing-buckingham-winslow"
            },
            {
                "property_type" : "apartment",
                "type" : "maisonette",
                "url": "https://www.alexanderandco.co.uk/property-search/maisonettes-available-to-rent-in-aylesbury-bicester-dunstable-wing-buckingham-winslow",
            },
        ]
        for item in start_urls:
            formdata = {
                "location": "",
                "price[]": "",
                "price[]": "",
                "bedrooms": "",
                "building": item["type"],
                "option": "com_startek",
                "view": "results",
                "task": "search",
                "Itemid": "114",
                "search_type": "L",
                "includesold": "0",
            }
            yield FormRequest(
                url=item["url"],
                callback=self.parse,
                formdata=formdata,
                dont_filter=True,
                meta={
                    "property_type":item["property_type"],
                    "base_url":item["url"]
                }
            )
    

    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='card__content-body']/h4/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True

        if page == 2 or seen:
            base_url = response.meta["base_url"]
            p_url = base_url + f"/page-{page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1, "property_type":response.meta["property_type"], "base_url":base_url})
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Ella_Co_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/text()", input_type="F_XPATH", split_list={" in ":1})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h1/text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='section__content']//p//text()", input_type="M_XPATH")
        # ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//div[@class='section__content']//p//text()[contains(.,'AVAILABLE')]", input_type="M_XPATH", split_list={"AVAILABLE":1, " ":0, " ":1})
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//div[@class='section__content']//p//text()[contains(.,'EPC RATING:')]", input_type="M_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//aside//i[contains(@class,'bedroom')]/parent::li//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//aside//i[contains(@class,'bathroom')]/parent::li//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//h4/span[contains(@class,'price-q')]//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'startekDetailsMap')]/text()", input_type="F_XPATH", split_list={"startekDetailsMap":1, "$, '":1, "'":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'startekDetailsMap')]/text()", input_type="F_XPATH", split_list={"','":1, "'":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='modalimage']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//div[@id='modalfloorplan']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="pets_allowed", input_value="//div[@class='section__content']//p//text()[contains(.,'Pets considered')]", input_type="M_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Alexander & Co", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="01869 326696", input_type="VALUE")

        item_loader.add_value("external_id", response.url.split("/")[-1])
        
        city = response.xpath("//h1/text()").get()
        if city:
            if "," in city:
                city = city.split(",")[-2]
            item_loader.add_value("city", city.split(" in ")[-1])
        available_date = "".join(response.xpath("//div[@class='section__content']//p//text()[contains(.,'- AVAILABLE')]").getall())
        if available_date:
            available_date = available_date.split("- AVAILABLE")[-1].strip().split(" ")
            date_parsed = dateparser.parse(available_date[0]+ " "+available_date[1], date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        yield item_loader.load_item()
