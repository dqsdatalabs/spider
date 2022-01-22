# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from ..loaders import ListingLoader 
import re
import scrapy


class HydesSpider(scrapy.Spider):
    name = 'hydes_co_uk' 
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source = "hydescouk_PySpider_united_kingdom_en"

    start_urls = ["https://hydes.co.uk/search/?showstc=on&showsold=on&instruction_type=Letting"]

    def parse(self, response):
        for listing in response.xpath("//div[contains(@class,'property ')]"):
            f_url = listing.xpath(".//a/@href[contains(.,'property')]").get()
            status = listing.xpath(".//div[contains(@class,'type')]/text()").get()
            if not "agreed" in status.lower():
                yield scrapy.Request(response.urljoin(f_url), callback=self.get_info)

    def get_info(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        dontallow=response.xpath("//div[@id='property-carousel']//text()").getall()
        for i in dontallow:
            if "let" in i.lower():
                return 
        item_loader.add_value("title", response.css("title::text").get())
        description = "\n".join([i.strip() for i in response.css(".results-description::text").extract() if i.strip()] + response.css(".results-description>p::text").extract())
        item_loader.add_value("description", description.strip())
        address = [i.strip() for i in response.css(".thumbs::text").extract() if i.strip()][0]
        item_loader.add_value("city", re.split("[,-]", [i.strip() for i in response.css(".thumbs::text").extract() if i.strip()][0])[-3].strip())
        item_loader.add_value("address", address)
        coordinates = response.css("footer + script::text").get().split("renderStreetview(opt,")[-1].split(",")[:2]
        item_loader.add_value("latitude", coordinates[0].strip())
        item_loader.add_value("longitude", coordinates[1].strip())
        description = description.lower()
        if "apartment" in description or "flat" in description:
            item_loader.add_value("property_type", "apartment")
        elif "home" in description:
            item_loader.add_value("property_type", "house")
        elif "house" in description:
            item_loader.add_value("property_type", "house")
        elif "bungalow" in description:
            item_loader.add_value("property_type", "house")
        elif "cottage" in description:
            item_loader.add_value("property_type", "house")
        elif "maisonette" in description:
            item_loader.add_value("property_type", "house")

        features = response.css(".bullets-result>li::text").extract()
        for feature in features:
            if "sq ft" in feature:
                item_loader.add_value("square_meters", int(re.sub(r"[^\d]", "", feature)))
        rooms = {k: v for k, v in zip(response.css('div[class="col-sm-5 col-md-4 rooms"]>img::attr(alt)').extract(), [int(i.strip()) for i in response.css('div[class="col-sm-5 col-md-4 rooms"]::text').extract() if i.strip()])}
        item_loader.add_value("room_count", rooms["bedrooms"])
        if "baths" in rooms:
            item_loader.add_value("bathroom_count", rooms["baths"])
        images =  [response.urljoin(i) for i in response.css('#property-carousel .img-responsive::attr(src)').extract()]
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))
        floor_plan_path = response.css("#property-floorplans>img::attr(src)").extract()
        if floor_plan_path:
            item_loader.add_value("floor_plan_images", [response.urljoin(i) for i in floor_plan_path])
            item_loader.add_value("external_images_count", len(images) + len(floor_plan_path))
        item_loader.add_value("rent", re.sub(r"[^\d]", "", address.split("-")[-1]))
        item_loader.add_value("currency", "GBP")
        item_loader.add_value("landlord_name", self.name)
        item_loader.add_value("landlord_phone", response.css(".white-large::text").get())
        item_loader.add_value("landlord_email", response.css('.contact-info>p:contains("email us")>a::attr(href)').get().split(":")[-1])
        yield item_loader.load_item()