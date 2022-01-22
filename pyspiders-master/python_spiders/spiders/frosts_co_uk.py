# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
import scrapy
from ..loaders import ListingLoader 
import datetime
from word2number import w2n
 
class FrostsSpider(scrapy.Spider):
    name = "frosts_co_uk"
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    current_page = 1
    base_url = "https://www.frosts.co.uk/properties-to-rent/all-properties/!/page/1"

    def start_requests(self):
        yield scrapy.Request(self.base_url, callback=self.parse)

    def parse(self, response):
        for listing in response.css('div[class="list_item property rental"]'):
            if listing.css(".let_agreed::text").get() == "Let Agreed":
                continue
            yield scrapy.Request(response.urljoin(listing.css(".main_image>a::attr(href)").get()), callback=self.more_info)
        # recursively going through pagination
        if response.css("#autopagi_not_so_auto_loader::text").get():
            self.current_page += 1
            yield scrapy.Request(re.sub(r"\d+$", str(self.current_page), self.base_url), callback=self.parse)

    def more_info(self, response):
        data = ListingLoader(response=response)
        data.add_value("external_link", response.url)
        data.add_value("external_id", response.url.split("-")[-1])
        data.add_value("title", response.css("title::text").get().strip())
        description = "".join(response.css("#property_description *::text").extract()).strip()
        data.add_value("description", description)
        raw_info = [i[4:-3] for i in response.css("main").xpath("//comment()").extract()]
        images = [raw_info.pop(i) for i in range(len(raw_info)) if "property-images" in raw_info[i]][0].split('"')[1].split(",")
        data.add_value("external_images_count", len(images))
        info = {k.split(":")[0].strip(): k.split(":")[1].replace('"', "").strip() for k in raw_info}
        data.add_value("images", images)
        floor_plan = response.css("#floorplan_cycle img::attr(src)").get()
        if floor_plan:
            data.add_value("floor_plan_images", [floor_plan])
            data.add_value("external_images_count", len(images) + 1)
        data.add_value("city", info["property-city"])
        data.add_value("zipcode", info["property-zipcode"])
        data.add_value("address", info["property-address"])
        data.add_value("latitude", info["property-latitude"])
        data.add_value("longitude", info["property-longitude"])
        property_type = response.css(".property-header__details__blurb::text").get().lower()
        if ("house" in property_type) or ("cottage" in property_type) or ("bungalow" in property_type):
            data.add_value("property_type", "house")
        elif ("flat" in property_type) or ("maisonette" in property_type):
            data.add_value("property_type", "apartment")
        elif ("studio" in property_type):
            data.add_value("property_type", "studio")
        available = response.css(".property-header__details__blurb__available::text").get()
        if available:
            available = available.split()[-1]
            if available == "now":
                available = datetime.datetime.now()
            else:
                available = datetime.datetime.strptime(available, "%d/%m/%Y")
            data.add_value("available_date", datetime.datetime.strftime(available, "%Y-%m-%d"))
        room_count = int(info["property-bedrooms"].strip())
        if room_count:
            if room_count > 0:
                data.add_value("room_count", int(info["property-bedrooms"].strip()))
        else:
            room_count = response.xpath("//div[contains(@id,'property_description')]//text()[contains(.,'studio')]").get()
            if room_count:
                data.add_value("room_count", "1")
            else:
                room_count = response.xpath("//li[contains(.,'Bedroom')]//text()").get()
                if room_count:
                    room_count = room_count.strip().split(" ")[0]
                    if room_count.isdigit():
                        data.add_value("room_count", room_count)
                    else:
                        try:
                            data.add_value("room_count", w2n.word_to_num(room_count))
                        except :
                            pass
        data.add_value("rent", int(re.sub(r"[^\d]", "", info['property-price'])))
        data.add_value("currency", "GBP")
        data.add_value("landlord_name", response.css(".property-header2__contact__title::text").get())
        data.add_value("landlord_email", response.css(".property-header2__contact__email>a::text").get())
        data.add_value("landlord_phone", response.css(".property-header2__contact__tel::text").get())
        deposit = sum([int(re.sub("[^\d]+", "", i)) for i in response.css(".letting_details>span::text").extract()])
        if deposit:
            data.add_value("deposit", deposit)
        if response.css(".bathroom").extract():
            data.add_value("bathroom_count", 1)
        data.add_value("external_source", "FrostsCo_PySpider_united_kingdom_en")
        features = [i.strip().lower() for i in response.css(".list-group>ul>li::text").extract()]
        for feature in features:
            if "parking" in feature or "garage" in feature:
                data.add_value("parking", True)
            elif feature == "furnished":
                data.add_value("furnished", True)
            elif feature == "unfurnished":
                data.add_value("furnished", False)
        yield data.load_item()
