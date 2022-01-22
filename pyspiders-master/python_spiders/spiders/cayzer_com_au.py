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
    name = 'cayzer_com_au'      
    execution_type='testing'
    country='australia'
    locale='en'
    start_urls = ["https://www.cayzer.com.au/rent/"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='current']"):
            status = item.xpath(".//div[contains(@class,'status-tag')]/div/div/text()").get()
            if status and ("leased" in status.lower()):
                continue
            follow_url = item.xpath("./@href").get()
            yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//a[.='>']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
            )
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        p_type = "".join(response.xpath("//h2/b/text()").getall())
        if get_p_type_string(p_type):
            p_type = get_p_type_string(p_type)
            item_loader.add_value("property_type", p_type)
        else:
            p_type = "".join(response.xpath("//p[@class='prop-desc']//text()").getall())
            if get_p_type_string(p_type):
                p_type = get_p_type_string(p_type)
                item_loader.add_value("property_type", p_type)
            else:
                return

        item_loader.add_value("external_id", response.url.split("-")[-1].strip())

        item_loader.add_value("external_source", "Cayzer_Com_PySpider_australia")          
        item_loader.add_xpath("title","//h2/b/text()")
        item_loader.add_xpath("room_count", "//div[@class='col-xs-4 text-right']//img[@alt='Bedroom']/following-sibling::text()[1]")
        item_loader.add_xpath("bathroom_count", "//div[@class='col-xs-4 text-right']//img[@alt='Bathroom']/following-sibling::text()[1]")     
        rent = response.xpath("//div[b[.='Price']]/following-sibling::div[1]/text()").get()
        if rent:
            rent = rent.split("$")[-1].lower().split('p')[0].strip().replace(',', '')
            item_loader.add_value("rent", int(float(rent)) * 4)
        item_loader.add_value("currency", 'AUD')
        city = response.xpath("//div[@class='row']/div/div[@class='col-xs-4']/text()").get()
        if city:
            item_loader.add_value("city", city.strip())
        address = response.xpath("//div[@class='row']/div/div[@class='col-xs-4 text-center']/text()").get()
        if address:
            if city:
                address= address.strip()+", "+city.strip()
            item_loader.add_value("address", address.strip())
        parking = response.xpath("//div[@class='col-xs-4 text-right']//img[@alt='Car']/following-sibling::text()[1]").get()
        if parking:
            item_loader.add_value("parking", True) if parking.strip() != "0" else item_loader.add_value("parking", False)
  
        available_date = response.xpath("//div[b[.='Date Available']]/following-sibling::div[1]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d %m %Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        description = " ".join(response.xpath("//p[@class='prop-desc']//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip().split("Contact:")[0])
        
        script_map = response.xpath("//script[contains(.,'var p = ')]/text()").get()
        if script_map:
            latlng = script_map.split("var p = '")[1].split("'")[0]
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())

        images = [x.split("url(")[1].split(");")[0] for x in response.xpath("//div[@class='slider']/div[@class!='item floorplan']/@style").getall()]
        if images:
            item_loader.add_value("images", images)
        floor_plan_images = [x.split("url(")[1].split(");")[0] for x in response.xpath("//div[@class='slider']/div[@class='item floorplan']/@style").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
      
        item_loader.add_xpath("landlord_name", "//div[@class='agent-list']//a[@class='agent-name']/b/text()")
        item_loader.add_xpath("landlord_phone", "//div[@class='agent-list']/div/span[contains(.,'P')]/following-sibling::text()[1]")
        item_loader.add_xpath("landlord_email", "//div[@class='agent-list']//span[.='E']/following-sibling::a/text()[1]")
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "cottage" in p_type_string.lower()):
        return "house"
    else:
        return None