# Author: Abbad49606a

import re
import scrapy
from ..loaders import ListingLoader 


class AadsPropertyManagementSpider(scrapy.Spider):
    name = 'aads_property_management'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    start_urls = ["https://aadspropertymanagement.com/notices?c=44&p=1"]
    current_page = 1

    def parse(self, response):
        for listing in response.css('.proerty_list_e>a::attr(href)').extract():
            yield scrapy.Request(response.urljoin(listing), callback=self.get_info)

        if response.css('.page:contains("next")').get():
            self.current_page += 1
            yield scrapy.Request(re.sub(r"\d+$", str(self.current_page), self.start_urls[0]), callback=self.parse)

    def get_info(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", f"{''.join(self.name.split('_'))}_PySpider_{self.country}_{self.locale}")
        item_loader.add_value("title", response.css("title::text").get())

        description = "\n".join(response.css("#full_notice_description p::text").extract())
        if not description:
            description = response.css("#full_notice_description::text").get().strip()
        item_loader.add_value("description", description.strip())
        address = response.css(".property_location::text").get()
        city = address.split(",")
        if len(city) >= 2:
            item_loader.add_value("city", city[-2].strip())
        if ',' in address:
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
        else:
            zipcode = ' '.join(address.split(' ')[-2:]).strip()
            if any(c for c in zipcode if c.isdigit()):
                item_loader.add_value('zipcode', zipcode)
        item_loader.add_value("address", address)
        property_type = response.css('span:contains("Property Type")::text').get()
        if property_type:
            property_type = property_type.lower().split(":")[-1].strip()
            if property_type in ["apartment", "flat"]:
                item_loader.add_value("property_type", "apartment")
            elif "room" in property_type:
                item_loader.add_value("property_type", "room")
            elif property_type in ['semi detached', 'semi detached bungalow', 'terraced', 'terraced bungalow', 'town house', 'villa', 'bungalow', "farmhouse"]:
                item_loader.add_value("property_type", "house")
            elif property_type == "studio":
                item_loader.add_value("property_type", "studio")
            else:
                return
        elif "studio" in response.css(".property_name::text").get():
            item_loader.add_value("property_type", "studio")
        else:
            return
        bedrooms = response.css('span:contains("Bedrooms")::text').get()
        if bedrooms:
            item_loader.add_value("room_count", int(re.sub(r"[^\d]", "", bedrooms)))
        bathrooms = response.css('span:contains("Bathrooms")::text').get()
        if bathrooms:
            item_loader.add_value("bathroom_count", int(re.sub(r"[^\d]", "", bathrooms)))
        for feature in response.css(".amenities_li span::text").extract():
            feature = feature.strip().lower()
            if "furnished" in feature:
                item_loader.add_value("furnished", True)
            if "unfurnished" in feature:
                item_loader.add_value("furnished", False)
        images = [i.split("url('")[-1].split(");")[0] for i in response.css(".property_slider_img::attr(style)").extract()]
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))
        rent = response.css("#price_value::text").get()
        if "pw" in rent:
            item_loader.add_value("rent", int(re.sub(r"[^\d]", "", rent)) * 4)
        else:
            item_loader.add_value("rent", int(re.sub(r"[^\d]", "", rent)))
        item_loader.add_value("currency", "GBP")
        item_loader.add_value("landlord_name", self.name)
        item_loader.add_value("landlord_phone", "0044 07341822081")
        item_loader.add_value("landlord_email", "summ@aadspropertymanagement.com")
        yield item_loader.load_item()