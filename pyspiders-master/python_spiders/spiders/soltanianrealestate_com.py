# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class SoltanianrealestateSpider(Spider):
    name = 'soltanianrealestate_com'
    country='canada'
    locale='en' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.soltanianrealestate.com"]
    start_urls = ["https://soltanianrealestate.com/Brokerage-Listings"]
    
    
    def parse(self, response):
        site_pages = {}
        
        for url in response.css("div.nnl_listingImages:contains('For Lease') a::attr(href)").getall():
            site_pages[url] = url
        for url in site_pages:    
            yield Request(response.urljoin(site_pages[url]), callback=self.populate_item, dont_filter = True)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = response.css("td.LP_Label:contains('Type:') + td.LP_GlanceBoxDetails::text").get()
        property_type = property_type.strip().lower()
        if(property_type == "com - commercial/retail"):
            return

        property_type = "apartment"

        external_id = response.css("div.LP_DetailsHeaderDetaile2::text").get().split(":")[1].strip()
        
        rent = response.css("div.LP_DetailsHeaderPrice::text").get()
        rent = re.findall("[0-9]+", rent)
        rent = "".join(rent)
        currency = "CAD"
        
        address = response.css("div#dvAddress::text").get()
        city = address.split(",")[-1]
        zipcode = address.split(",")[-2]
        title = address

        description = response.css("tr#trad_text td.LP_CntntValues::text").getall()
        description = " ".join(description).strip()
        
        square_meters =  response.css("td.LP_CntntTitles:contains('Approximatly Square Footage:') + td.LP_CntntValues::text").get()
        if(square_meters):
            square_meters = square_meters.split("-")[0].strip()
            square_meters = int(int(square_meters)/10.763)
        
        room_count = response.css("td.LP_CntntTitles:contains('Rooms:') + td.LP_CntntValues::text").get().strip()
        
        furnished = response.css("td.LP_CntntTitles:contains('Furnished:') + td.LP_CntntValues::text").get().strip()
        if("N" in furnished):
            furnished = False
        else:
            furnished = True

        parking = response.css("td.LP_CntntTitles:contains('Garage Type:') + td.LP_CntntValues::text").get().strip()
        if(parking):
            parking = True
        else:
            parking = False

        balcony = response.css("td.LP_CntntTitles:contains('Balcony:') + td.LP_CntntValues::text").get()
        if(balcony):
            balcony = True
        else:
            balcony = False

        pets_allowed = response.css("td.LP_CntntTitles:contains('Pet Permited:') + td.LP_CntntValues::text").get()
        if(pets_allowed):
            if("N" in pets_allowed):
                pets_allowed = False
            else:
                pets_allowed = True
        else:
            pets_allowed = False

        images = response.css("div.item img.imageNoSave::attr(src)").getall()
        images = [response.urljoin(image_src) for image_src in images]

        script_location = response.css("script:contains('new google.maps.LatLng')::text").get()

        latitude = re.findall("new google.maps.LatLng\((-?[0-9]+\.[0-9]+),-?[0-9]+\.[0-9]+\)", script_location)[0]
        longitude = re.findall("new google.maps.LatLng\(-?[0-9]+\.[0-9]+,(-?[0-9]+\.[0-9]+)\)", script_location)[0]

        landlord_name = "SOLTANIAN REAL ESTATE"
        landlord_phone = "416-901-8881"
        landlord_email = "sharon@soltanianrealestate.com"

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)
        item_loader.add_value("square_meters", int(int(square_meters)*10.764))
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("parking", parking)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("pets_allowed", pets_allowed)
        item_loader.add_value("images", images)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
       
        yield item_loader.load_item()
