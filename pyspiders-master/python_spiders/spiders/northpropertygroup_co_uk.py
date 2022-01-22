# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin 
import re

class MySpider(Spider):
    name = 'northpropertygroup_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.northpropertygroup.co.uk/rent/properties/?wpv_view_count=816&wpv_post_search=&wpv-wpcf-rental-price_min=&wpv-wpcf-rental-price_max=&wpv-property-bedrooms=&wpv-wpcf-address-city=&wpv-wpcf-property-type=Apartment&wpv_filter_submit=Apply+Filters",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.northpropertygroup.co.uk/rent/properties/?wpv_view_count=816&wpv_post_search=&wpv-wpcf-rental-price_min=&wpv-wpcf-rental-price_max=&wpv-property-bedrooms=&wpv-wpcf-address-city=&wpv-wpcf-property-type=Duplex+Apartment&wpv_filter_submit=Apply+Filters",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.northpropertygroup.co.uk/rent/properties/?wpv_view_count=816&wpv_post_search=&wpv-wpcf-rental-price_min=&wpv-wpcf-rental-price_max=&wpv-property-bedrooms=&wpv-wpcf-address-city=&wpv-wpcf-property-type=Studio&wpv_filter_submit=Apply+Filters",
                "property_type" : "studio"
            },
            {
                "url" : "https://www.northpropertygroup.co.uk/rent/properties/?wpv_view_count=816&wpv_post_search=&wpv-wpcf-rental-price_min=&wpv-wpcf-rental-price_max=&wpv-property-bedrooms=&wpv-wpcf-address-city=&wpv-wpcf-property-type=Studio+Apartment&wpv_filter_submit=Apply+Filters",
                "property_type" : "studio"
            },
            {
                "url" : "https://www.northpropertygroup.co.uk/rent/properties/?wpv_view_count=816&wpv_post_search=&wpv-wpcf-rental-price_min=&wpv-wpcf-rental-price_max=&wpv-property-bedrooms=&wpv-wpcf-address-city=&wpv-wpcf-property-type=New+Home&wpv_filter_submit=Apply+Filters",
                "property_type" : "apartment"
            },
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[contains(.,'More Details')]/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
        
        next_page = response.xpath("//a[contains(.,'Next')]/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type" : response.meta.get("property_type")},
            )
        

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Northpropertygroup_PySpider_" + self.country + "_" + self.locale)
        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        externalid=response.xpath("//link[@rel='shortlink']/@href").get()
        if externalid:
            item_loader.add_value("external_id",externalid.split("p=")[-1])
        
        rent=response.xpath("//ul[contains(@class,'uk-list-divider')]/li[contains(.,'£')]//text()").get()
        if rent:
            price=rent.split("£")[1].split(" ")[0].replace(",","")
            item_loader.add_value("rent_string", price+"£")
        
        room_count=response.xpath("//ul[contains(@class,'uk-list-divider')]/li[contains(.,'Bed')]//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("Bed")[0].strip())
        elif response.xpath("//ul[contains(@class,'uk-list-divider')]/li[contains(.,'Studio')]//text()").get():
            item_loader.add_value("room_count", "1")
        
        address = response.xpath("//p/span[contains(@uk-icon,'locat')]/parent::p/text()").get()
        if address:
            item_loader.add_value("address", address)
            if "," in address:
                zipcode = address.split(",")[-1].strip()
                status = False
                if not zipcode.isalpha():
                    for i in zipcode:
                        if i.isdigit():
                            status = True
                    if status:
                        item_loader.add_value("zipcode", zipcode)


        city=response.xpath("//h1[@class='uk-margin uk-h1']/text()").get()
        if city:
            item_loader.add_value("city",city.split(",")[-1])
        addresscheck=item_loader.get_output_value("address")
        if not addresscheck:
            adress=" ".join(response.xpath("//h2[.='Property Address']/following-sibling::div/div/div/div[2]/p/text()").getall())
            if adress:
                item_loader.add_value("address",adress)
        titlecheck=item_loader.get_output_value("title")
        if not titlecheck:
            title=response.xpath("//title//text()").get()
            if title:
                item_loader.add_value("title",title)

        
        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng("')[1].split('",')[0]
            longitude = latitude_longitude.split('LatLng("')[1].split('", "')[1].split('")')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        desc="".join(response.xpath("//ul/li/h2[contains(.,'Property Description')]/parent::li/div//text()").getall())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}','',desc))
        if "dishwasher" in desc.lower():
            item_loader.add_value("dishwasher", True)
        if "washing machine" in desc.lower():
            item_loader.add_value("washing_machine", True)
        if ("furnished" in desc.lower()) and ("unfurnished" not in desc.lower()):
            item_loader.add_value("furnished", True)
        if "lift" in desc.lower():
            item_loader.add_value("elevator", True)
        if "balcony" in desc.lower():
            item_loader.add_value("balcony", True)
        if "terrace" in desc.lower():
            item_loader.add_value("terrace", True)
        
        square_meters = response.xpath("//li[contains(.,'sq ft')]/span/text()").get()
        if square_meters:
            square_meters = square_meters.split(".")[0].strip()
            sqm = str(int(int(square_meters)* 0.09290304))
            item_loader.add_value("square_meters", sqm)        
        elif "sq ft" in desc.lower():
            square_m=desc.split("sq ft")[0].strip().split(" ")[-1].replace(",","")
            sqm = str(int(int(square_m)* 0.09290304))
            item_loader.add_value("square_meters", sqm)
        
        images=[x for x in response.xpath("//ul/li/h2[contains(.,'Property Gallery')]/parent::li/div//div/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        item_loader.add_value("landlord_name", "NORTH PROPERTY GROUP")
        item_loader.add_value("landlord_phone", "44 0 113 4264 444")
        item_loader.add_value("landlord_email", "hello@northpropertygroup.co.uk")
        
        status = response.xpath("//li/span[contains(@uk-icon,'bookmark')]/parent::li/span/text()").get()
        if status and "To Let" in status:
            yield item_loader.load_item()

