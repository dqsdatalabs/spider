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
    name = 'silverbulletrealty_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'       
    
    def start_requests(self):
        start_url = "https://www.silverbulletrealty.com.au/silver-bullet-realty-listings/"
        yield Request(start_url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@data-elementor-type='single']"):
            url = item.xpath(".//a[contains(.,'More')]/@href").get()
            room = item.xpath(".//div[@class='bed']/text()").get()
            bathroom = item.xpath(".//div[@class='bath']/text()").get()
            parking = item.xpath(".//div[@class='car']/text()").get()
            yield Request(response.urljoin(url), callback=self.populate_item, meta={"room":room, "bathroom":bathroom,"parking":parking,})
    
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        status_rent = response.xpath("//strong[contains(.,'Rent:')]").get()
        property_type = "".join(response.xpath("//h4[contains(.,'Property description')]/following-sibling::text()").getall())
        if status_rent and property_type:
            if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
            else: return
        else: return
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Silverbulletrealty_Com_PySpider_australia")  

        external_id = response.xpath("//link[@rel='shortlink']/@href").get()
        if external_id: item_loader.add_value("external_id", external_id.split("p=")[-1].strip())

        item_loader.add_xpath("title", "//h2/text()")        
        item_loader.add_xpath("city", "//div[strong[.='Suburb: ']]/text()")      
        address = response.xpath("//h2/text()").get()
        if address:
            item_loader.add_value("address", address.strip())      
        item_loader.add_value("room_count", response.meta.get('room'))
        item_loader.add_value("bathroom_count", response.meta.get('bathroom'))
        item_loader.add_xpath("deposit", "//p/strong[.='Bond:']/following-sibling::text()[1]") 
        square_meters = response.xpath("//div[strong[.='Building Area: ']]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", int(float(square_meters)))
        rent = response.xpath("//p/strong[.='Rent:']/following-sibling::text()[1]").get()
        if rent:
            rent = rent.split("$")[1].split("p")[0].strip().replace(",","")
            item_loader.add_value("rent", int(float(rent)) * 4)
        item_loader.add_value("currency", 'AUD')
 
        parking = response.meta.get('parking')
        if parking:
            item_loader.add_value("parking", True) if parking.strip() != "0" else item_loader.add_value("parking", False)
        balcony = response.xpath("//div[h4[.='Property description']]//text()[contains(.,'Balcony ') or contains(.,' balconies')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        available_date = response.xpath("//div[h4[.='Property description']]//text()[contains(.,'AVAILABLE')]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split("AVAILABLE")[-1].strip(), date_formats=["%d %m %Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        description = " ".join(response.xpath("//div[h4[.='Property description']]//text()[.!='Property description']").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
     
        images = [x for x in response.xpath("//div[@id='gallery-1']//a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_xpath("landlord_name", "//div[@id='conper']/div/b/text()[1]")
        item_loader.add_xpath("landlord_phone", "substring-after(//div[@id='conper']/div/text()[contains(.,'Phone: ')],'Phone: ')")
        item_loader.add_xpath("landlord_email", "//div[@id='conper']/div/a/text()[contains(.,'@')]")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None