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
    name = 'innovate_ea_co_uk'
    execution_type = "testing"
    country = "united_kingdom"
    locale = "en"
    thousand_separator = ','
    scale_separator = '.'  
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.innovate-ea.co.uk/?id=26699&action=view&route=search&view=list&input=DY4&jengo_radius=10&jengo_property_for=2&jengo_property_type=8&jengo_category=1&jengo_min_beds=0&jengo_max_beds=9999&jengo_min_price=0&jengo_max_price=99999999999&jengo_order=6&pfor_complete=&pfor_offer=&trueSearch=&searchType=postcode&latitude=&longitude=#total-results-wrapper",
                    "https://www.innovate-ea.co.uk/?id=26699&action=view&route=search&view=list&input=DY4&jengo_radius=10&jengo_property_for=2&jengo_property_type=11&jengo_category=1&jengo_min_beds=0&jengo_max_beds=9999&jengo_min_price=0&jengo_max_price=99999999999&jengo_order=6&pfor_complete=&pfor_offer=&trueSearch=&searchType=postcode&latitude=&longitude=#total-results-wrapper",
                    "https://www.innovate-ea.co.uk/?id=26699&action=view&route=search&view=list&input=DY4&jengo_radius=10&jengo_property_for=2&jengo_property_type=18&jengo_category=1&jengo_min_beds=0&jengo_max_beds=9999&jengo_min_price=0&jengo_max_price=99999999999&jengo_order=6&pfor_complete=&pfor_offer=&trueSearch=&searchType=postcode&latitude=&longitude=#total-results-wrapper",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.innovate-ea.co.uk/?id=26699&action=view&route=search&view=list&input=DY4&jengo_radius=10&jengo_property_for=2&jengo_property_type=6&jengo_category=1&jengo_min_beds=0&jengo_max_beds=9999&jengo_min_price=0&jengo_max_price=99999999999&jengo_order=6&pfor_complete=&pfor_offer=&trueSearch=&searchType=postcode&latitude=&longitude=#total-results-wrapper",
                    "https://www.innovate-ea.co.uk/?id=26699&action=view&route=search&view=list&input=DY4&jengo_radius=10&jengo_property_for=2&jengo_property_type=7&jengo_category=1&jengo_min_beds=0&jengo_max_beds=9999&jengo_min_price=0&jengo_max_price=99999999999&jengo_order=6&pfor_complete=&pfor_offer=&trueSearch=&searchType=postcode&latitude=&longitude=#total-results-wrapper",
                    "https://www.innovate-ea.co.uk/?id=26699&action=view&route=search&view=list&input=DY4&jengo_radius=10&jengo_property_for=2&jengo_property_type=10&jengo_category=1&jengo_min_beds=0&jengo_max_beds=9999&jengo_min_price=0&jengo_max_price=99999999999&jengo_order=6&pfor_complete=&pfor_offer=&trueSearch=&searchType=postcode&latitude=&longitude=#total-results-wrapper",
                    "https://www.innovate-ea.co.uk/?id=26699&action=view&route=search&view=list&input=DY4&jengo_radius=10&jengo_property_for=2&jengo_property_type=13&jengo_category=1&jengo_min_beds=0&jengo_max_beds=9999&jengo_min_price=0&jengo_max_price=99999999999&jengo_order=6&pfor_complete=&pfor_offer=&trueSearch=&searchType=postcode&latitude=&longitude=#total-results-wrapper",
                    "https://www.innovate-ea.co.uk/?id=26699&action=view&route=search&view=list&input=DY4&jengo_radius=10&jengo_property_for=2&jengo_property_type=14&jengo_category=1&jengo_min_beds=0&jengo_max_beds=9999&jengo_min_price=0&jengo_max_price=99999999999&jengo_order=6&pfor_complete=&pfor_offer=&trueSearch=&searchType=postcode&latitude=&longitude=#total-results-wrapper",
                    "https://www.innovate-ea.co.uk/?id=26699&action=view&route=search&view=list&input=DY4&jengo_radius=10&jengo_property_for=2&jengo_property_type=15&jengo_category=1&jengo_min_beds=0&jengo_max_beds=9999&jengo_min_price=0&jengo_max_price=99999999999&jengo_order=6&pfor_complete=&pfor_offer=&trueSearch=&searchType=postcode&latitude=&longitude=#total-results-wrapper",
                    "https://www.innovate-ea.co.uk/?id=26699&action=view&route=search&view=list&input=DY4&jengo_radius=10&jengo_property_for=2&jengo_property_type=19&jengo_category=1&jengo_min_beds=0&jengo_max_beds=9999&jengo_min_price=0&jengo_max_price=99999999999&jengo_order=6&pfor_complete=&pfor_offer=&trueSearch=&searchType=postcode&latitude=&longitude=#total-results-wrapper",
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
        for item in response.xpath("//div[@class='col-md-12']"):
            follow_url = response.urljoin(item.xpath(".//a[@class='link-actual-result']/@href").get())
            rent = item.xpath(".//h1[@class='actual-property-price']/text()").get()
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"],"rent":rent})

        next_page = response.xpath("//a[@class='next-prev']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]})    
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        rent = response.meta.get('rent')
        if rent and rent.strip() != "£0":
            item_loader.add_value("rent_string", rent) 

        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_id", response.url.split("?")[0].split("property/")[-1].split("/")[0])
        item_loader.add_value("external_source", "Innovate_Ea_Co_PySpider_united_kingdom")
        item_loader.add_xpath("title", "//div/h1/text()")        
        address =response.xpath("//div/h1/text()").extract_first()
        if address:
            item_loader.add_value("address",address.strip())
            item_loader.add_value("zipcode",address.split(",")[-1].strip())
        city =response.xpath("//ul[@class='overview-list']/li/h6[contains(.,'Area Size')]/span/text()").extract_first()
        if city:
            item_loader.add_value("city",city.strip())
          
        available_date = response.xpath("//ul[@class='overview-list']/li/h6[contains(.,'Available Date')]/span/text()").extract_first()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d %B %Y"], languages=['en'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
     
        parking = response.xpath("//div[@id='features']/text()[contains(.,'Parking')]").extract_first()
        if parking:   
            item_loader.add_value("parking",True) 

        pets = response.xpath("//ul[@class='overview-list']/li/h6[contains(.,'Pets')]/span/text()").extract_first()
        if pets:   
            if "no" in pets.lower():
                item_loader.add_value("pets_allowed",False)
            else:
                item_loader.add_value("pets_allowed",True) 
   
        room_count = response.xpath("//ul[@class='overview-list']/li/h6[contains(.,'Bedrooms')]/span/text()").extract_first()
        if room_count:   
            if "studio" in room_count.lower():
                item_loader.add_xpath("room_count","1")
            else:
                item_loader.add_xpath("room_count",room_count)
        item_loader.add_xpath("bathroom_count","//ul[@class='overview-list']/li/h6[contains(.,'Bathrooms')]/span/text()")

        images = [response.urljoin(x) for x in response.xpath("//div[@class='fotorama']/a/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)
        script_map = response.xpath("//script[@type='text/javascript']/text()[contains(.,' prop_lat =')]").get()
        if script_map:
            item_loader.add_value("latitude", script_map.split("prop_lat =")[1].split(";")[0].strip())
            item_loader.add_value("longitude", script_map.split("prop_lng = ")[1].split(";")[0].strip())
       
        desc = " ".join(response.xpath("//div[@id='description-tab']/div/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
 
        item_loader.add_value("landlord_name", "Innovate Estate Agents")
        item_loader.add_value("landlord_phone", "0121 559 0065")
        item_loader.add_value("landlord_email", "info@innovate-ea.co.uk")  

        yield item_loader.load_item()