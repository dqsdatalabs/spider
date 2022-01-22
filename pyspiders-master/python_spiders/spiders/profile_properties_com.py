import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader
import math


class ProfilePropertiesComSpider(scrapy.Spider):
    name = 'profile_properties_com'
    allowed_domains = ['profile-properties.com']
    start_urls = ['https://profile-properties.com/properties/rentals.html']
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        for appartment in response.css("a.uk-position-cover::attr(href)").extract():
            url = "https://profile-properties.com" + appartment
            yield Request(url,
                          callback=self.populate_item,
                          )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        id = response.css(
            "#je-tab1 > div > div.uk-width-medium-2-3.je-col1 > div > h4::text").get().split("REF")[1]

        title = response.css(
            '#je-item-wrapper > div:nth-child(4) > h1::text').get()
        address = response.css(
            '#je-item-wrapper > div.je-item-fields-container.cd-margin-bottom.uk-clearfix > div > div > span:nth-child(7)::text').get()
        zipcode = address.split(',')[-1]

        rent = response.css(
            '#jd-tab4 > div.je-item-box-price > span::text').get().split("$ ")[1]

        if "," in rent:
            rent = rent.split(",")[0] + rent.split(",")[1]

        if "." in rent:
            rent = rent.split(".")[0]

        description = response.css(
            '#je-tab1 > div > div.uk-width-medium-2-3.je-col1 > div.uk-panel.cd-panel-box > div.je-item-box-introtext').get().replace('\\r\\n', ' ').split('CONTACT INFO')[0]
        space = description.split('Square Footage:')[
            1].split('Parking Spaces')[0].split(">")[1].split('<')[0]
        space = int(int(space)/10.7639)

        feats = response.css(
            '#je-tab1 > div > div.uk-width-medium-2-3.je-col1 > div > div > ul > li::text').extract()

        dishwasher = None
        laundry = None
        balcony = None
        for item in feats:
            if "Dishwasher" in item:
                dishwasher = True
            elif "Washer/Dryer" in item:
                laundry = True
            elif "Balcony" in item:
                balcony = True

        images = response.xpath(
            '//img[@u="thumb"]/@src').extract()

        bedrooms = response.css(
            '#je-item-wrapper > div.je-item-fields-container.cd-margin-bottom.uk-clearfix > div > div > div.uk-display-block.uk-float-left.cd-margin-right.cd-customfield-row.cd-beds > span::text').get()

        if 'Two' in bedrooms:
            bedrooms = 2

        bathrooms = response.css(
            '#je-item-wrapper > div.je-item-fields-container.cd-margin-bottom.uk-clearfix > div > div > div.uk-display-block.uk-float-left.cd-margin-right.cd-customfield-row.cd-baths > span::text').get()

        if 'One' in bathrooms:
            bathrooms = 1

        coords = response.css('script:contains("lat:")').get()
        lat = coords.split('lat:')[1].split(',')[0]
        lng = coords.split('lng:')[1].split(',')[0]

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", id)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", "apartment")
        item_loader.add_value("room_count", bedrooms)
        item_loader.add_value("bathroom_count", math.ceil(float(bathrooms)))
        item_loader.add_value("address", address)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("square_meters", int(int(space)*10.764))
        item_loader.add_value("dishwasher", dishwasher)
        item_loader.add_value("washing_machine", laundry)
        item_loader.add_value("balcony", balcony)

        item_loader.add_value("latitude", lng.strip())
        item_loader.add_value("longitude", lat.strip())

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("currency", "CAD")

        # LandLord Details
        item_loader.add_value("landlord_phone", "(604) 464-7548")
        item_loader.add_value("landlord_email", "info@profile-properties.com")
        item_loader.add_value("landlord_name", "Profile Properties LTD.")

        yield item_loader.load_item()
