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
    name = 'rodiprofessionals_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://rodiprofessionals.com.au/property-search/page/1/?property-id=&location=any&status=for-rent&type=apartment&bedrooms=any&bathrooms=any&min-price=any&max-price=any&min-area=&max-area=",
                    "http://rodiprofessionals.com.au/property-search/page/1/?property-id&location=any&status=for-rent&type=unit&bedrooms=any&bathrooms=any&min-price=any&max-price=any&min-area&max-area",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://rodiprofessionals.com.au/property-search/page/1/?property-id=&location=any&status=for-rent&type=duplex-semi&bedrooms=any&bathrooms=any&min-price=any&max-price=any&min-area=&max-area=",
                    "http://rodiprofessionals.com.au/property-search/page/1/?property-id=&location=any&status=for-rent&type=house&bedrooms=any&bathrooms=any&min-price=any&max-price=any&min-area=&max-area=",
                    "http://rodiprofessionals.com.au/property-search/page/1/?property-id=&location=any&status=for-rent&type=townhouse&bedrooms=any&bathrooms=any&min-price=any&max-price=any&min-area=&max-area=",
                    "http://rodiprofessionals.com.au/property-search/page/1/?property-id=&location=any&status=for-rent&type=terrace&bedrooms=any&bathrooms=any&min-price=any&max-price=any&min-area=&max-area=",
                    "http://rodiprofessionals.com.au/property-search/page/1/?property-id=&location=any&status=for-rent&type=villa&bedrooms=any&bathrooms=any&min-price=any&max-price=any&min-area=&max-area=",
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

        page = response.meta.get("page", 2)
        seen = False

        for item in response.xpath("//article[contains(@class,'property')]//a[contains(.,'More Detail')]/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        if page == 2 or seen: 
            base_url = f"http://rodiprofessionals.com.au/property-search/page/{page}/?"
            yield Request(base_url + response.url.split('?')[1], callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Rodiprofessionals_Com_PySpider_australia")    
        external_id = response.xpath("//link[@rel='shortlink']/@href").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("p=")[-1])
            
        item_loader.add_xpath("title","//div[@class='wrap clearfix']/p//text()")
        address= ""
        address =response.xpath("//div[@class='content clearfix']/p[1]//text()[contains(.,' — ')]").get()
        if address:
            address = address.split("—")[1].strip()
            item_loader.add_value("address", address )
        else:
            address =response.xpath("//div[@class='wrap clearfix']/p//text()").get()
            if address:
                item_loader.add_value("address", address.strip())
    
        city = response.xpath("//div[@class='wrap clearfix']/p//text()[contains(.,' - ')]").get()
        if city:
            item_loader.add_value("city", city.split("-")[-1].strip())
        
        if address:
            zipcode = address.strip().split(" - ")[0]
            zipcode_value = " ".join(zipcode.strip().split(" ")[-2:])
            if not zipcode_value.replace(" ","").isalpha():
                item_loader.add_value("zipcode", zipcode_value)
            
        room_count = response.xpath("//span[i[@class='icon-bed']]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("Bed")[0].strip())
        
        bathroom_count = response.xpath("//span[i[@class='icon-bath']]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("Bath")[0].strip())
        
        rent = response.xpath("//div[@class='content clearfix']/p[1]//text()[contains(.,'$')]").get()
        if rent:
            rent = rent.split("$")[-1].lower()
            if "week" in rent:
                rent = rent.split('p')[0].split("/")[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)) * 4))
        else:
            rent = response.xpath("//h5[@class='price']/span[2]/text()").get()
            if rent:
                item_loader.add_value("rent", rent)
        item_loader.add_value("currency", 'AUD')
        available_date = response.xpath("//div[@class='content clearfix']/p[1]//text()[contains(.,'Available')]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split("Available")[1].split("—")[0], date_formats=["%d %m %Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        description = " ".join(response.xpath("//div[@class='content clearfix']/p//text()[not(contains(.,'For Lease —'))]").getall()) 
        if description:
            item_loader.add_value("description", description.strip())

        images = [x for x in response.xpath("//div[@id='property-detail-flexslider']//li/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        script_map = response.xpath("//script[contains(.,'new google.maps.LatLng(')]/text()").get()
        if script_map:
            latlng = script_map.split("new google.maps.LatLng(")[1].split(");")[0]
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())
        parking = response.xpath("//span[i[@class='icon-garage']]/text()[.!='0 Garages']").get()
        if parking:
            item_loader.add_value("parking", True)
    
        item_loader.add_value("landlord_name", "Rodi Realty")
        item_loader.add_value("landlord_phone", "(02) 9646 4999")
        item_loader.add_value("landlord_email", "info@rodiprofessionals.com.au")
 
        yield item_loader.load_item()