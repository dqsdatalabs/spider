# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
import json
import scrapy
from ..loaders import ListingLoader

class CentralEstates(scrapy.Spider):
    name = "central_estates"
    execution_type = 'testing' 
    country = 'united_kingdom' 
    locale = 'en'
    current_page = 1
    custom_settings = {
        "HTTPCACHE_ENABLED": False,
    }

    def start_requests(self):
        yield scrapy.Request(self.make_url(), callback=self.parse) 
 
    def parse(self, response):
        for listing in response.css('div[class="panel panel-default"]'):
            data = ListingLoader(response=response)
            external_link = response.urljoin(listing.css("a::attr(href)").get())
            data.add_value("external_link", external_link)

            external_id=listing.css("a::attr(href)").get().split("/")[2]
            if external_id=="rps_cee-WAL210509":
                data.add_value("property_type","apartment")
            if external_id=="rps_cee-WAL210731":
                data.add_value("property_type","house")
        

            data.add_value("external_id", listing.css("a::attr(href)").get().split("/")[2])
            data.add_value("room_count", int(listing.css(".beds-number::text").get().strip()))
            data.add_value("bathroom_count", int(listing.css(".bath-number::text").get().strip()))
            data.add_value("rent", int(re.sub(r"[^\d]", "", listing.css(".thumb-description-height a")[-1].css("::text").get())))
            data.add_value("currency", "GBP")
            data.add_value("landlord_name", self.name)
            data.add_value("landlord_email", "enquiries@central-estates.co.uk")
            data.add_value("landlord_phone", "0208 520 0077")
            yield scrapy.Request(external_link, callback=self.more_info, meta={"data": data})
        if self.current_page <= max([int(i) for i in response.css("ul.pagination a::text").extract() if i.isdigit()]):
            yield scrapy.Request(self.make_url(), callback=self.parse)

    def more_info(self, response):
        data = response.meta.get("data")
        data.add_value("title", response.css("title::text").get())
        data.add_value("city", [i.strip() for i in [i.lower() for i in response.css("h2>span::text").extract() if i.strip()][0].split(",")if i.strip()][-1])
        description = response.css("p.margin-top-30::text").get()
        data.add_value("description", description)
        coordinates = json.loads(response.css("body::attr(onload)").get().split("setLocratingIFrameProperties(")[-1].split(");")[0].replace("'", '"'))
        data.add_value("latitude", coordinates["lat"])
        data.add_value("longitude", coordinates["lng"]) 
        
        description = description.lower() 
        if "hmo" in description:
            property_type = "student_apartment"

        elif "flat" in description or "flat." in description:
            property_type = "apartment" 
            data.add_value("property_type", property_type)
        
        # index=description.find("flat")
        # if index:
        #     property_type = "apartment"

        else:
            property_type = "house"
            
        if "studio" in [i.lower() for i in response.css("aside li::text").extract()]:
            property_type = "studio"
        data.add_value("address", response.css("h2 > span::text").get().strip().strip(","))
        images = ["https://www.central-estates.co.uk" + i for i in response.css('#property-thumbnails>.carousel-inner img::attr(src)').extract()]
        data.add_value("images", images)
        data.add_value("external_images_count", len(images))
        temp = response.css('#property-floorplans>img::attr(src)').extract()
        if temp:
            floor_plan_images = ["https://www.central-estates.co.uk" + i for i in temp]
            data.add_value("floor_plan_images", floor_plan_images)
            data.add_value("external_images_count", len(images) + len(floor_plan_images))
        data.add_value("external_source", 'CentralEstates_PySpider_united_kingdom_en')

        for feature in [i.strip("*").lower() for i in response.css(".details-links-borders::text").extract()]:
            feature = feature.lower().strip()
            if "approx" in feature:
                data.add_value("square_meters", int(re.sub(r"[^\d]", "", feature)))
            elif feature == "parking":
                data.add_value("parking", True)
            elif feature == "unfurnished":
                data.add_value("furnished", False)
            elif "furnished" in feature:
                data.add_value("furnished", True)
            elif "floor" in feature:
                data.add_value("floor", feature)
            elif "studio" in feature:
                property_type = "studio"

            
            data.add_value("property_type", property_type)

        yield data.load_item()
    def make_url(self):
        self.current_page += 1
        return "https://www.central-estates.co.uk/search/" + str(self.current_page - 1) + ".html?showstc=on&showsold=on&instruction_type=Letting&minprice=&maxprice="
