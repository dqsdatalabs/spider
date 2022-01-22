# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request, FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json


class MySpider(Spider):
    name = 'draker_co_uk'
    external_source = "Draker_PySpider_united_kingdom"
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    }

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.draker.co.uk/lettings/search/results/ajax?",
                ],
            },
        ]  # LEVEL 1

        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    headers=self.headers
                )
    # 1. FOLLOWING

    def parse(self, response, **kwargs):

        page = response.meta.get("page", 2)
        seen = False
        base_url = "https://www.draker.co.uk"

        datas = json.loads(response.body)["data"]

        for data in datas["properties"]:

            property_type = data['property_types_name']
            external_id = data['properties_id']
            follow_url = data['properties_url']
            if follow_url:
                follow_url = base_url + follow_url
            address = data['properties_address_formatted']
            city = data['properties_town']
            zipcode = data['properties_postcode']
            longitude = data['properties_longitude']
            latitude = data['properties_latitude']
            bathroom_count = data['properties_bathrooms']
            room_count = data['properties_bedrooms']
            rent = data['properties_full_price_pcm_formatted']
            available_date = data['properties_lettings_date_available']
            description = data['properties_summary']
            landlord_email = data['branches_email']
            landlord_phone = data['branches_main_tel']

            for image in data['properties_photos']:
                images = base_url+image['properties_images_url']
            for floor_plan_images in data['properties_floorplans']:
                floor_plan_images = base_url + \
                    floor_plan_images['properties_images_url']

            yield Request(follow_url, callback=self.populate_item, meta={"external_id": external_id, "follow_url": follow_url, "address": address, "city": city, "zipcode": zipcode, "longitude": longitude, "latitude": latitude, "property_type": property_type, "bathroom_count": bathroom_count, "room_count": room_count, "rent": rent, "available_date": available_date, "description": description, "landlord_email": landlord_email, "landlord_phone": landlord_phone, "images": images, "floor_plan_images": floor_plan_images, })

            seen = True

        if page == 2 or seen:
            f_url = f"https://www.draker.co.uk/lettings/search/results/ajax?&page={page}"
            print(f_url)
            yield Request(f_url, callback=self.parse, meta={"page":page+1})

    # 2. SCRAPING level 2

    def populate_item(self, response):

        item_loader = ListingLoader(response=response)

        property_type = response.meta.get("property_type")
        external_id = response.meta.get("external_id")
        follow_url = response.meta.get("follow_url")
        address = response.meta.get("address")
        city = response.meta.get("city")
        zipcode = response.meta.get("zipcode")
        longitude = response.meta.get("longitude")
        latitude = response.meta.get("latitude")
        bathroom_count = response.meta.get("bathroom_count")
        room_count = response.meta.get("room_count")
        rent = response.meta.get("rent")
        available_date = response.meta.get("available_date")
        description = response.meta.get("description")
        images = response.meta.get("images")
        floor_plan_images = response.meta.get("floor_plan_images")
        landlord_email = response.meta.get("landlord_email")
        landlord_phone = response.meta.get("landlord_phone")

        item_loader.add_value("external_source", self.external_source)

        if property_type:
            item_loader.add_value('property_type', property_type)

        item_loader.add_value('title', address)

        if external_id:
            item_loader.add_value('external_id', external_id)

        if follow_url:
            item_loader.add_value('external_link', follow_url)

        if address:
            item_loader.add_value('address', address)

        if city:
            item_loader.add_value('city', city)

        if zipcode:
            item_loader.add_value('zipcode', zipcode)

        if description:
            item_loader.add_value('description', description)

        if longitude:
            item_loader.add_value('longitude', longitude)

        if latitude:
            item_loader.add_value('latitude', latitude)

        if bathroom_count:
            item_loader.add_value('bathroom_count', bathroom_count)

        if room_count:
            item_loader.add_value('room_count', room_count)

        if rent:
            rent = rent.split("£")[1]
            rent = rent.replace(",", ".")
            item_loader.add_value('rent', rent)
        item_loader.add_value('currency', "GBP")

        if available_date:
            item_loader.add_value('available_date', available_date)

        if images:
            item_loader.add_value('images', images)

        if floor_plan_images:
            item_loader.add_value('floor_plan_images', floor_plan_images)

            item_loader.add_value('landlord_name', 'Draker')

            if landlord_email:
                item_loader.add_value('landlord_email', landlord_email)

            if landlord_phone:
                item_loader.add_value('landlord_phone', landlord_phone)

        yield item_loader.load_item()
