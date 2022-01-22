# Author: Abbad49606a

from ..loaders import ListingLoader 
import re
from python_spiders.helper import format_date
import scrapy


class CityRoomRentalsSpider(scrapy.Spider):
    name = 'city_room_rentals'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    start_urls = ["https://www.cityroomrentals.co.uk/?s=&city=&_available_start_date=&_available_start_date=&property_type=&bedrooms=&guests="]

    def parse(self, response):
        for listing in response.css('.overview-property-container'):
            property_type = listing.css(".property-style::text").get()
            yield scrapy.Request(listing.css('a::attr(href)').get(), callback=self.get_info, meta={"property_type": property_type})

    def get_info(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", f"{''.join(self.name.split('_'))}_PySpider_{self.country}_{self.locale}")
        item_loader.add_value("title", response.css("title::text").get())

        description = "\n".join([i.strip() for i in response.css("#property-description::text").extract() if i.strip()])
        if not description:
            description = "\n".join([i.strip() for i in response.css("#property-description>p::text").extract() if i.strip()])
        item_loader.add_value("description", description.strip())
        item_loader.add_value("address", response.css("h1::text").get())
        coordinates =  re.search(r"(?<=LatLng\()([\d\.-]+), ([\d\.-]+)(?=\);)", response.css(".container+script::text").get()).groups()
        for temp in response.css('div[class="property-information clearfix"]>.colored-medium-dark-gray'):
            if temp.css('i[class="fa fa-map-marker"]'):
                city = temp.css("::text").get()
                if len(city.split(",")) > 2:
                    item_loader.add_value("city", [i.strip() for i in city.split(",") if i.strip()][-1])
        item_loader.add_value("latitude", coordinates[0])
        item_loader.add_value("longitude", coordinates[1])
        property_type = response.meta.get("property_type")
        if "student" in description:
            item_loader.add_value("property_type", "student_apartment")
            #item_loader.add_value("room_count", 1)
        elif "apartment" in property_type.lower():
            item_loader.add_value("property_type", "apartment")
            #item_loader.add_value("room_count", 1)
        else:
            item_loader.add_value("property_type", "studio")
            #item_loader.add_value("room_count", 0)

        room_count = response.xpath("//span[contains(.,'bedroom')]/text()").re_first(r'\d+')
        if room_count:
            item_loader.add_value("room_count", room_count)

        images = response.css(".slider-for .portrait_image::attr(src)").extract()
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))
        try:
            item_loader.add_value("rent", int(re.sub(r"[^\d]", "", response.css(".masthead-price>h2::text").get().split(".")[0])) * 4)
        except:
            pass
        for feature in response.css(".property-features li>span::text").extract():
            feature = feature.lower()
            if "parking" in feature or "park" in feature or "bike storage" in feature:
                item_loader.add_value("parking", True)
        item_loader.add_value("currency", "GBP")
        item_loader.add_value("landlord_name", self.name)
        item_loader.add_value("landlord_email", "bookings@cityroomrentals.co.uk")
        item_loader.add_value("landlord_phone", "+353 1699 4401")
        yield item_loader.load_item()