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
    name = 'bresicwhitney_com_au'
    execution_type='testing'
    country='australia'
    locale='en'

    def start_requests(self):
        start_url = "https://bresicwhitney.com.au/rent/search"
        yield Request(start_url, callback=self.parse)
    
    def parse(self, response):

        for item in response.xpath("//div[@class='tiles']/article/div[@class='tile-content']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = "".join(response.xpath("//section[@id='highlights']//text()").getall())
        if property_type:
            if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
            else: return
        rented = response.xpath("//div[@class='price']//text()").get()
        if rented and "deposit taken" in rented.lower():
            return
        prp_type = response.xpath("//dl[@class='specs-list']/dt[.='Property type']/following-sibling::dd[1]/text()").get()
        if prp_type:
            if "medical" in prp_type.lower() or "retail" in prp_type.lower():
                return
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("?")[0].split("-")[-1].strip())
        item_loader.add_value("external_source", "Bresicwhitney_Com_PySpider_australia")          
        title =response.xpath("//meta[@property='og:title']/@content").get()
        if title:
            if "Garage" in title:
                return
            item_loader.add_value("title", title.strip())
        city =response.xpath("//div[@class='suburb']//text()").get()
        if city:
            item_loader.add_value("city", city.strip())
        address = response.xpath("//div[@class='address']//text()").get()
        if address:
            address= address.strip()+", "+city
            item_loader.add_value("address", address.strip())

        room_count = response.xpath("//ul[@class='property-features']/li[@class='bed']//span[@class='number']/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        elif "studio" in get_p_type_string(property_type):
            item_loader.add_value("room_count","1")
        bathroom_count = response.xpath("//ul[@class='property-features']/li[@class='bath']//span[@class='number']/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",int(float(bathroom_count)))
        rent = response.xpath("//div[@class='price']//text()[.!='Contact Agent']").get()
        if rent:
            if "per week" in rent:
                rent = rent.split("$")[-1].split('p')[0].strip().replace(',', '')
                item_loader.add_value("rent", int(float(rent)) * 4)
            else:
                item_loader.add_value("rent_string", rent.replace(",",""))
        item_loader.add_value("currency", 'USD')
        deposit = response.xpath("//dl[@class='specs-list']/dt[.='Bond']/following-sibling::dd[1]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace(",",""))
        parking = response.xpath("//ul[@class='property-features']/li[@class='car']//span[@class='number']/text()").get()
        if parking:
            if parking.strip() == "0":
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        balcony = response.xpath("//ul/li[contains(.,'balcony ') or contains(.,'Balcony ')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        furnished = response.xpath("//dl[@class='specs-list']/dt[.='Furnished']/following-sibling::dd[1]/text()").get()
        if furnished:
            if furnished.lower().strip() == "no":
                item_loader.add_value("furnished", False)
            elif furnished.lower().strip() == "yes":
                item_loader.add_value("furnished", True)
        pets_allowed = response.xpath("//dl[@class='specs-list']/dt[.='Pets allowed']/following-sibling::dd[1]/text()").get()
        if pets_allowed:
            if pets_allowed.lower().strip() == "no":
                item_loader.add_value("pets_allowed", False)
            elif pets_allowed.lower().strip() == "yes":
                item_loader.add_value("pets_allowed", True)

        available_date = response.xpath("//dl[@class='specs-list']/dt[.='Available date']/following-sibling::dd[1]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d %m %Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        description = " ".join(response.xpath("//section[@id='highlights']//text()[.!='Highlights'][normalize-space()]").getall()) 
        if description:
            item_loader.add_value("description", description.strip())

        images = [x for x in response.xpath("//figure[@class='photo']/picture/img/@data-lazy").getall()]
        if images:
            item_loader.add_value("images", images)
        floor_plan_images = [x for x in response.xpath("//figure[@class='floorplan']/picture/img/@data-lazy").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        item_loader.add_xpath("landlord_name", "normalize-space(//ul[@id='contacts']//h3[@class='staff-name']/text())")
        item_loader.add_xpath("landlord_phone", "normalize-space(//ul[@id='contacts']//dd[@class='staff-mobile']/a[@itemprop='telephone']/text())")
 
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "home" in p_type_string.lower()):
        return "house"
    else:
        return None