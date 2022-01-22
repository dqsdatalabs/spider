# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from ..loaders import ListingLoader 
import re
import json
from python_spiders.helper import format_date
from scrapy.http import FormRequest
import scrapy


class ColemanEstatesSpider(scrapy.Spider):
    name = 'coleman_estates'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source = "colemanestates_PySpider_united_kingdom_en",
    base_url = "https://www.colemanestates.com/wp-admin/admin-ajax.php"

    form_data = {
  
        "action": "get_properties_ajax",
        "term": "undefined",
        "price_min": "0",
        "price_max": "2000000",
        "bedrooms_min": "",
        "bedrooms_max": "",
        "property_type": "",
        "category": "4",
    }

    def start_requests(self):
        yield FormRequest(self.base_url, callback=self.parse, formdata=self.form_data)

    def parse(self, response):
        for listing in json.loads(response.body):
            yield scrapy.Request(listing["permalink"], callback=self.get_info, meta={"data": listing})

    def get_info(self, response):
        little_data = response.meta.get("data")
        item_loader = ListingLoader(response=response)
        if "for-sale" in response.url or "sold" in response.url: return
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("title", response.css("title::text").get())

        description = "\n".join(response.css('div[class="accordion--block--text page--text"]>p::text').extract())
        item_loader.add_value("description", description.strip())
        address = response.xpath("//div[contains(@class,'single--property__right')]//div[contains(@class,'heading--medium mb-2')]/text()").get()
        if address:
            address = address.replace("For Sale","").replace("To Let","").replace("Sold","").strip()
            zipcode = address.split(",")[-1]
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", zipcode)
        description = description.lower()

        property_type = response.xpath("//div[contains(@class,'tagline') and contains(@class,'color')]/text()").get()
        if "home" in property_type.lower() or "house" in property_type.lower() or "bungalow" in property_type.lower() or "villa" in property_type.lower():
            item_loader.add_value("property_type", "house")
        elif "apartment" in property_type.lower() or "flat" in property_type.lower():
            item_loader.add_value("property_type", "apartment")
        elif "studio" in property_type.lower():
            item_loader.add_value("property_type", "studio")
        else: return

        item_loader.add_value("room_count", int(re.sub(r"[^\d]", "", little_data["bedrooms"])))
        item_loader.add_value("bathroom_count", int(re.sub(r"[^\d]", "", little_data["bathrooms"])))

        from datetime import datetime
        import dateparser
        available_date = little_data["date"]
        if available_date:
            if "now" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        images = response.css(".gallery-slide noscript>img::attr(src)").extract()
        item_loader.add_value("images", list(set(response.css(".inlinetest::attr(href)").extract())))
        item_loader.add_value("external_images_count", len(images))
        item_loader.add_value("rent", int(re.sub(r"[^\d]", "", little_data["price"])))
        item_loader.add_value("currency", "GBP")
        item_loader.add_value("landlord_name", self.name)
        item_loader.add_value("landlord_phone", "01952 244990")
        item_loader.add_value("landlord_email", "sales@colemanestates.com")

        deposit = response.xpath("//text()[contains(.,'Deposit:')]").get()
        if deposit:
            deposit = deposit.split("Deposit:")[1].split(".")[0].split(",")[0]
            item_loader.add_value("deposit", deposit)

        parking = response.xpath("//text()[contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)

        city = response.xpath("//h1/text()").get()
        if city:
            if "sale" in city.lower() or "sold" in city.lower():
                return
            
            city = city.split(",")[1].strip()
            item_loader.add_value("city", city)

        if not item_loader.get_collected_values("city"):
            city = response.xpath("//div[@class='heading--medium mb-2']/text()").get()
            if city: item_loader.add_value("city", city.split(',')[-2].strip())         

        status = response.xpath("//div[contains(@class,'single--property__right')]//div[contains(@class,'heading--medium mb-2')]/text()[contains(.,' Sale')]").get()
        if not status:
            yield item_loader.load_item()