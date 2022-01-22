# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import re
import scrapy
from ..loaders import ListingLoader

class MchughAndCoSpider(scrapy.Spider):
    name = "mchugh_and_co"
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'

    base_url = "https://mchughandco.com/Lettings/Listing.aspx"

    def start_requests(self):
        yield scrapy.Request(self.base_url, callback=self.parse)

    def parse(self, response):
        for listing in response.css(".pic>a::attr(href)").extract():
            yield scrapy.Request("https://mchughandco.com/Lettings/" + listing, callback=self.more_info)

    def more_info(self, response):
        data = ListingLoader(response=response)
        data.add_value("external_link", response.url)
        data.add_value("external_id", response.url.split("?")[-1].split("&")[0].split("=")[-1])
        data.add_value("title", response.css("title::text").get().strip())
        description = "".join(response.css(".txtDescription *::text").extract()).strip().replace("\r\n", "").replace("\r\n", "")
        data.add_value("description", description)
        data.add_value("zipcode", response.css(".address::text").get().split(",")[-1].strip())
        data.add_value("address", response.css(".address::text").get().strip())
        
        city = "".join(response.xpath("//div[@class='address']/text()").getall())
        if city:
            city = city.split(",")[-2].strip()
            data.add_value("city", city)
            
        description = description.lower()
        if "hmo" in description:
            data.add_value("property_type", "student_apartment")
        elif "flat" in description:
            data.add_value("property_type", "apartment")
        else:
            data.add_value("property_type", "house")
        # can either be no of bedrooms or "studio"
        temp_type = response.css(".typebeds::text").get().strip().lower()
        if temp_type == "studio":
            data.add_value("property_type", temp_type)
            # since studio  does  not have a room, it is set to 0
            data.add_value("room_count", 0)
        else:
            data.add_value("room_count", int(response.css(".iconMenu .icon+li::text").get().strip()))
        data.add_value("bathroom_count", int(response.css(".iconMenu")[1].css(".icon+li::text").get().strip()))
        images = response.css('.thumbs img::attr(src)').extract()
        data.add_value("images", images)
        floor_plan_path = response.css("#mainContent_floorplanLink1::attr(href)").get()
        if floor_plan_path:
            data.add_value("floor_plan_images", ["https://mchughandco.com" + floor_plan_path[2:]])
            data.add_value("external_images_count", len(images) + 1)
        data.add_value("external_images_count", len(images))
        data.add_value("rent", int(re.sub(r"[^\d]", "", response.css(".price::text").get())) * 4)
        data.add_value("currency", "GBP")

        parking = response.xpath("normalize-space(//img[@id='mainContent_parkIcon']/../following-sibling::li[1]/text())").get()
        if parking:
            parking = int(parking)
            if parking > 0:
                data.add_value("parking", True)
            else:
                data.add_value("parking", False)
        else:
            data.add_value("parking", False)

        script_data = response.xpath("//script[contains(.,'maps.LatLng')]//text()").get()
        if script_data:
            latlng = script_data.split("maps.LatLng(")[1].split(");")[0].strip()
            lat = latlng.split(",")[0].strip()
            lng = latlng.split(",")[1].strip()
            if lat and lat != "0":
                data.add_value("latitude", lat)
                data.add_value("longitude", lng)

        data.add_value("landlord_name", self.name)
        data.add_value("landlord_email", "sales@mchughandcompany.co.uk")
        data.add_value("landlord_phone", "020 7485 0112")
        data.add_value("external_source", "".join(self.name.split()))
        yield data.load_item()
