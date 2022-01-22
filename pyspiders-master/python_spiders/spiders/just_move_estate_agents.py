# Author: Abbad49606a

from ..loaders import ListingLoader 
import re
import scrapy
import urllib
from python_spiders.helper import format_date
import datetime

class JustMoveEstateAgentsSpider(scrapy.Spider):
    name = 'just_move_estate_agents'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    start_urls = ["https://www.justmoveestates.co.uk/properties/?pageSize=12&orderBy=PriceSearchAmount&orderDirection=DESC&propind=L&businessCategoryId=1&searchType=grid&hideProps=&page=1"]
    current_page = 1

    def parse(self, response):
        for listing in response.css('div[class="gridprop col-md-3 col-sm-4"]'):
            yield scrapy.Request(response.urljoin(listing.css("h3>a::attr(href)").get()), callback=self.get_info)
        
        self.current_page += 1
        if self.current_page <= max(set([int(i.strip()) for i in response.css(".pagerpagenumbers *::text").extract() if i.strip().isdigit()])):
            yield scrapy.Request(re.sub(r"\d+$", str(self.current_page), self.start_urls[0]), callback=self.parse)

    def get_info(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", f"{''.join(self.name.split('_'))}_PySpider_{self.country}_{self.locale}")
        item_loader.add_value("title", response.css("title::text").get())
        description = "\n".join(response.css(".description::text").extract()).strip()
        description = description.lower()


        date = description.split("available")[-1][1:]
        if date:
            if len(date.split()) == 3:
                date = re.sub(r"(th|rd|st|nd)", "", date)
                date = date.split()
                date[1] = date[1].capitalize()
                date = " ".join(date)
                item_loader.add_value("available_date", datetime.datetime.strftime(datetime.datetime.strptime(date,"%d %B %Y"), '%Y-%m-%d'))

        if re.search(r"\d+ \d+ \d+[0-9a-zA-Z ]", description):
            print(description)
            description = re.sub(r"\n[^\n]+$", "", description)
        item_loader.add_value("description", description)

        address = response.css(".leftcolumn .address::text").get()
        item_loader.add_value("address", address)
        item_loader.add_value("city", address.split(",")[-1].strip())
        coordinates = response.css("#maplinkwrap>a::attr(href)").get()
        if coordinates:
            coordinates = urllib.parse.parse_qs(coordinates.split("?")[-1])
            item_loader.add_value("latitude", coordinates["lat"][0])
            item_loader.add_value("longitude", coordinates["lng"][0])
        property_type = response.css(".bedsWithTypePropType::text").get()
        if property_type:
            property_type = property_type.lower()
            if "house" in property_type or "maisonette" in property_type:
                item_loader.add_value("property_type", "house")
            elif "apartment" in property_type or "flat" in property_type:
                item_loader.add_value("property_type", "apartment")
            elif "room" in property_type or "land" in property_type:
                item_loader.add_value("property_type", "room")
        item_loader.add_value("room_count", int(response.css(".beds::text").get()))
        bath = response.css(".bathrooms::text").get()
        if bath:
            item_loader.add_value("bathroom_count", int(bath))
        for feature in response.css(".features li::text").extract():
            feature = feature.lower()
            if feature == "unfurnished":
                item_loader.add_value("furnished", False)
            elif feature == "furnished":
                item_loader.add_value("furnished", True)
        external_id = re.search(r"[\dA-Z]+$", response.css(".reference::text").get())
        if external_id:
            item_loader.add_value("external_id", external_id.group())
        images = response.css(".propertyimagelist img::attr(src)").extract()
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))
        item_loader.add_value("rent", int(re.sub(r"[^\d]", "", response.css(".displayprice::text").get())))
        item_loader.add_value("currency", "GBP")
        item_loader.add_value("landlord_name", self.name)
        item_loader.add_value("landlord_phone", "0121 377 88 99")
        item_loader.add_value("landlord_email", "erdington@justmoveestates.co.uk")
        yield item_loader.load_item()