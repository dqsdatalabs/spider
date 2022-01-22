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
    name = 'caleproperty_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        yield Request(
            "https://www.caleproperty.com.au/search?disposalMethod=rent&searchKeyword=ID%2C+Suburb+or+Postcode&city=&priceMinimum=0&priceMaximum=0&bedrooms=&bathrooms=&carspaces=&search=&_fn=quicksearch&sid=4883&vid=4863", 
            callback=self.parse,
            )

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'propertyListItem')]"):
            follow_url = response.urljoin(item.xpath(".//a[contains(.,'View Detail')]/@href").get())
            property_type = " ".join(item.xpath(".//div[@class='listingIntro']//text()").getall())
            if get_p_type_string(property_type): yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(property_type)})

        next_button = response.xpath("//li[@class='next']/a/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse)
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Caleproperty_Com_PySpider_australia")
        
        square_meters = response.xpath("//td[contains(.,'Building')]/following-sibling::td/text()").get()
        if square_meters:
            return
        
        item_loader.add_xpath("title", "//h3[contains(@class,'detailHeading')]/text()")
        
        address = "".join(response.xpath("//span[@class='listingSuburb']/text() | //span[@class='listingStreet']/text()").getall())
        if address:
            item_loader.add_value("address", address.strip())

        zipcode = " ".join(response.xpath("substring-after(//script/text()[contains(.,'var address = ')],'address = ')").extract())
        if zipcode:
            zipcode = zipcode.split("var propertyID")[0].strip().replace('"','').strip().split(",")[-1].strip()
            item_loader.add_value("zipcode", zipcode.strip())
        
        city = response.xpath("//span[@class='listingSuburb']/text()").get()
        if city:
            item_loader.add_value("city", city.strip())
        
        rent = response.xpath("//h3[@class='detailPrice']/text()[contains(.,'$')]").get()
        if rent:
            price = rent.strip().split(" ")[0].replace("$","")
            item_loader.add_value("rent", int(float(price))*4)
        item_loader.add_value("currency", "AUD")
        
        room_count = response.xpath("//div[contains(@class,'BBCContentWrapper')][1]/h4/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//div[contains(@class,'BBCContentWrapper')][2]/h4/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        import dateparser
        available_date = response.xpath("//h4[contains(.,'Available from')]/text()").get()
        if available_date:
            available_date = available_date.split("from")[1].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        parking = response.xpath("//div[contains(@class,'BBCContentWrapper')][3]/h4/text()").get()
        if parking:
            item_loader.add_value("parking", True)
            
        external_id = response.xpath("//td[contains(.,'ID')]/following-sibling::td/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        latitude_longitude = response.xpath("//style[contains(.,'map')]/text()").get()
        if latitude_longitude:
            latitude_longitude = latitude_longitude.split('url("')[1].split('")')[0]
            latitude = latitude_longitude.split('%7C')[-1].split(',')[0]
            longitude = latitude_longitude.split('%7C')[-1].split(',')[1].split('&')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        description = " ".join(response.xpath("//p[@class='pull-left']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        if "studio" in description.lower():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta["property_type"])
            
        images = [response.urljoin(x.split('(')[1].split(")")[0]) for x in response.xpath("//ul[@class='slides']//@style[contains(.,'url')]").getall()]
        if images:
            item_loader.add_value("images", images)
        
        name = response.xpath("//h3[@class='detailAgentName']/text()").get()
        if name:
            item_loader.add_value("landlord_name", name)
        
        phone = response.xpath("//td[@class='agentContentLabel']/a/text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone)
        
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