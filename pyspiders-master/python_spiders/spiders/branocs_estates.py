# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
import scrapy
from ..loaders import ListingLoader

class BranocsEstatesSpider(scrapy.Spider):
    name = 'branocs_estates'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    start_urls = ["https://www.branocsestates.co.uk/Search?listingType=6&statusids=1%2C9%2C10%2C6%2C2%2C7%2C8%2C99%2C5&statusids=1&obc=Price&obd=Descending&areainformation=&radius=&bedrooms=&minprice=&maxprice=&page=1"]
    current_page = 1

    def parse(self, response):
        for listing in response.css('a[class="hexButtons hexButtonH"]::attr(href)').extract():
            yield scrapy.Request(response.urljoin(listing), callback=self.get_info)

        self.current_page += 1
        total_listings = int(re.search(r"^\d+", response.css(".title::text").get()).group())
        if (self.current_page - 1) * 12 <= total_listings:
            yield scrapy.Request(re.sub(r"\d+$", str(self.current_page), self.start_urls[0]), callback=self.parse)

    def get_info(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])
        item_loader.add_value("external_source", f"{''.join(self.name.split('_'))}_PySpider_{self.country}_{self.locale}")
        item_loader.add_value("title", response.css("title::text").get())
        description =  "\n".join([i.strip() for i in response.css(".fullDetailWrapper>article *::text").extract() if i.strip()])
        item_loader.add_value("description", description.strip())
        address = response.css(".fullDetailBody>h3::text").get()
        city = address.split(",")[-2]
        item_loader.add_value("zipcode", response.css("title::text").get().split(",")[-1].strip())
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        description = description.lower()
        if "house" in description:
            item_loader.add_value("property_type", "house")
        elif "maisonette" in description.lower():
            item_loader.add_value("property_type", "house")
        elif "apartment" in description or "flat" in description:
            item_loader.add_value("property_type", "apartment")
        elif "studio" in description or "office" in description:
            item_loader.add_value("property_type", "studio")
        elif "room" in description:
            item_loader.add_value("property_type", "room")
        for feature in response.css(".keyFeat span::text").extract():
            feature = feature.lower()
            if "garage" in feature:
                item_loader.add_value("parking", True)
            elif "parking" in feature:
                item_loader.add_value("parking", True)
        item_loader.add_value("room_count", int(re.sub(r"[^\d]", "", response.css('span:contains("bedrooms")::text').get())))
        item_loader.add_value("bathroom_count", int(re.sub(r"[^\d]", "", response.css('span:contains("bathrooms")::text').get())))
        images = response.css("#property-photos-device2 .rsImg::attr(href)").extract()
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))
        item_loader.add_value("rent", re.sub(r"[^\d]+", "", [i.strip() for i in response.css(".fdPrice > div::text").extract() if i.strip()][0]))
        item_loader.add_value("currency", "GBP")
        item_loader.add_value("landlord_name", self.name)
        item_loader.add_value("landlord_email", " info@branocsestates.co.uk")
        item_loader.add_value("landlord_phone", "01376 386555")
        
        status = response.xpath("//div[@class='priceStatus']/text()[contains(.,'Let Agreed')]").get()
        if not status:
            yield item_loader.load_item()