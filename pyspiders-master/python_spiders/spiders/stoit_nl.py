# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from geopy.geocoders import Nominatim
import dateparser

class MySpider(Spider):
    name = 'stoit_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'en' # LEVEL 1

    def start_requests(self):
        start_urls = [
            {"url": "https://www.stoit.nl/en/search?type-of-residence=23&furnished=All&surface=All&bedrooms=All&min-price=All&max-price=All&sort_by=field_geo_location_latlon", "property_type": "apartment"},
            {"url": "https://www.stoit.nl/en/search?type-of-residence=25&furnished=All&surface=All&bedrooms=All&min-price=All&max-price=All&sort_by=field_geo_location_latlon", "property_type": "house"},
            {"url": "https://www.stoit.nl/en/search?type-of-residence=24&furnished=All&surface=All&bedrooms=All&min-price=All&max-price=All&sort_by=field_geo_location_latlon", "property_type": "house"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            "base_url":url.get('url')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='view-content']/div//div[contains(@class,'field-name-field-images')]//a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Stoit_PySpider_" + self.country + "_" + self.locale)
        

        external_id = response.xpath("//div[@class='field-label' and contains(.,'Ref')]/following-sibling::div//text()").extract_first()
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("external_link", response.url)

        desc = "".join(response.xpath("//div[@class='field-item even']/p/text()").extract())
        item_loader.add_value("description", desc.strip())


        city = response.xpath("//div[@class='field-item even']/h2/text()").get()
        if city:
            item_loader.add_value("city", city)
            region = response.xpath("//div[@class='field-item even']/h1/text()").get()
            if region:
                address = region + " " + city
                #item_loader.add_value("address", address)
                item_loader.add_value("title", address)
                


        square_meters = response.xpath("//div[@class='field field-name-field-size-m2 field-type-number-integer field-label-inline clearfix']/div[@class='field-items']//text()").get()
        if square_meters:
            square_meters = square_meters.strip("m²")
        item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//div[@class='field field-name-rooms field-type-ds field-label-inline clearfix']/div[@class='field-items']//text()").get()
        if room_count:
            room_count = room_count.split(" ")[0]
        item_loader.add_value("room_count", room_count)

        available_date = response.xpath("//div[@class='field field-name-available-from-date field-type-ds field-label-inline clearfix']/div[@class='field-items']//text()[.!='Today' and .!='Rented']").get()
        if available_date:
            date_parsed = dateparser.parse(
                available_date, date_formats=["%d %B %Y"]
            )
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)
        
        images = [response.urljoin(x) for x in response.xpath("//ul[@class='slides']/li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        
        price = response.xpath("//div[@class='field field-name-price field-type-ds field-label-inline clearfix']/div[@class='field-items']//text()[.!='On request']").get()
        if price and "€" in price:
            price = price.strip("€").strip()
        item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")
        
        floor = response.xpath("//div[@class='field field-name-field-floor field-type-text field-label-inline clearfix']/div[@class='field-items']//text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        


        parking = response.xpath("//div[@class='field field-name-field-parking field-type-list-boolean field-label-inline clearfix']/div[@class='field-items']//text()").get()
        if parking:
            if parking.lower() == "yes" or (parking.isdigit() and parking != "0"):
                item_loader.add_value("parking", True)
            else:
                item_loader.add_value("parking", False)


        elevator = response.xpath("//div[@class='field field-name-field-elevator field-type-list-boolean field-label-inline clearfix']/div[@class='field-items']//text()").get()
        if elevator:
            if elevator.lower() == "yes":
                item_loader.add_value("elevator", True)
            else:
                item_loader.add_value("elevator", False)
        


        balconny = response.xpath("//div[@class='field field-name-field-balcony field-type-list-boolean field-label-inline clearfix']/div[@class='field-items']//text()").get()
        if balconny:
            if balconny.lower() == "yes":
                item_loader.add_value("balcony", True)
            else:
                item_loader.add_value("balcony", False)

        
        terrace = response.xpath("//div[@class='field field-name-field-terrace field-type-list-boolean field-label-inline clearfix']/div[@class='field-items']//text()").get()
        if terrace:
            if terrace.lower() == "yes":
                item_loader.add_value("terrace", True)
            else:
                item_loader.add_value("terrace", False)
        

        swimming_pool = response.xpath("//div[@class='field field-name-field-swimming-pool field-type-list-boolean field-label-inline clearfix']/div[@class='field-items']//text()").get()
        if swimming_pool:
            if swimming_pool.lower() == "yes":
                item_loader.add_value("swimming_pool", True)
            else:
                item_loader.add_value("swimming_pool", False)
        

        latLng = response.xpath("normalize-space(//script[contains(., 'lat')]/text())").get()
        if latLng:
            lat = latLng.split('"lat":')[1].split(",")[0].strip()
            lon = latLng.split('"lon":')[1].split(",")[0].strip()

            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lon)
        item_loader.add_xpath("address", "//div[@class='field-item even']/h2/text()")


        item_loader.add_value("landlord_phone", "+31 (0)88-5555040")
        item_loader.add_value("landlord_name", "Stoit Groep")
        item_loader.add_value("landlord_email", "info@stoit.nl")

        yield item_loader.load_item()
    

def ScriptToLatLng(latlng):
    latLngString = latlng.split("point")[2].strip("\",").split("\"icon")[0].strip(",")
    latitude = latLngString.split(",")[0].split(":")[1]
    longitude = latLngString.split(",")[1].split("lon\":")[1]
    return latitude,longitude
