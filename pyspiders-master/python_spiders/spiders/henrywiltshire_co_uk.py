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
    name = 'henrywiltshire_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom' 
    locale = 'en'
    external_source = "Henrywiltshire_PySpider_united_kingdom"

    def start_requests(self): 
        formdata = {
            "location": "",
            "search-type": "rent",
        }
        url = "https://www.henrywiltshire.co.uk/property-for-rent/united-kingdom/"
        yield FormRequest(
            url,
            callback=self.parse,
            formdata=formdata,
        )

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='property-box']/a"):
            status = item.xpath(".//div[@class='ribbon-wrapper']/div/text()").get()
            if status and "let" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
 
        item_loader.add_value("external_link", response.url)

        let=response.xpath("//h1/text()").get()

        if "let" in let.lower():
            return 

        f_text = " ".join(response.xpath("//h1//text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            f_text = " ".join(response.xpath("//div[@id='propdescgriditem']//text()").getall())
            if get_p_type_string(f_text):
                item_loader.add_value("property_type", get_p_type_string(f_text))
            else:
                return
        zipcode = response.xpath("//h1/text()").get()
        if zipcode:
            zipcode = zipcode.split(",")[-1].strip().split(" ")[-1]
            if not zipcode.isalpha():
                item_loader.add_value("zipcode", zipcode.split(",")[-1].strip().split(" ")[-1])
        item_loader.add_value("external_source", self.external_source)         
        item_loader.add_value("city", "London")
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//p[contains(.,'Property Ref')]/text()", input_type="F_XPATH", split_list={":":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/text()", input_type="F_XPATH", split_list={":":-1})
        # ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h1/text()", input_type="F_XPATH", split_list={" ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//section[@class='propertydesc']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//section[@class='property-features island']//li[@class='area']//i/text()", input_type="F_XPATH", get_num=True, sq_ft=True)
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//section[@class='property-features island']//li[@class='bedrooms']//i/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//section[@class='property-features island']//li[@class='bathrooms']//i/text()", input_type="F_XPATH", get_num=True)
        term = response.xpath("//section[@class='property-features island']//h3/text()").get()
        if term:
            if 'week' in term: ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//section[@class='property-features island']//h3/text()", input_type="F_XPATH", get_num=True, split_list={".":0}, per_week=True)
            elif 'month' in term: ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//section[@class='property-features island']//h3/text()", input_type="F_XPATH", split_list={".":0}, get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//section[@class='property-features island']//p[contains(.,'Available from')]/text()", input_type="F_XPATH", split_list={"from":1})
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//div[@class='marker']/@data-lat", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//div[@class='marker']/@data-lng", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'Parking') or contains(.,'parking') or contains(.,'Garage') or contains(.,'garage')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//strong[contains(.,'Furnished:')]/following-sibling::text()", input_type="F_XPATH", tf_item=True, tf_words={True:"furnished", False:"unfurnished"})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//div[@id='propenquire']/div[1]//h3/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[@id='propenquire']/div[1]//a[contains(@href,'tel')]/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="//div[@id='propenquire']/div[1]//a[contains(@href,'mailto')]/text()", input_type="F_XPATH")

        images = [response.urljoin(x.split('url(')[-1].split(')')[0].strip()) for x in response.xpath("//div[@id='propertypage-slider']/div/div[@class='rsBgimg']/@style").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        if not item_loader.get_collected_values("furnished"):
            ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Unfurnished')]", input_type="F_XPATH", tf_item=True, tf_value=False)
            ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Furnished')]", input_type="F_XPATH", tf_item=True)

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