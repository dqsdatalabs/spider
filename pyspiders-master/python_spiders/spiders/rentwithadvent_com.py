import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader
from ..user_agents import random_user_agent


class RentwithadventComSpider(scrapy.Spider):
    name = 'rentwithadvent_com'
    allowed_domains = ['rentwithadvent.com']
    start_urls = [
        'https://www.rentwithadvent.com/search/for-rent/any-city/unfurnished-or-furnished']
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        for appartment in response.css("#l-canvas > div > div > div > div > main > div > div > article > section.panel-pane.panel-pane--fuse-style.float-none.panel-pane--views.panel-pane--views-panes.panel-pane--property-listings-pane-property-listings > div > div > div.view-content > div"):
            url = "https://www.rentwithadvent.com/" + appartment.css(
                "div.views-fieldset.views-fieldset-property-image > a").attrib['href']
            yield Request(url,
                          callback=self.populate_item,
                          )

        next_page = response.css(
            'li.pager__item.pager__item--next>a').attrib['href']
        if next_page is not None:
            next_page = "https://www.rentwithadvent.com/"+next_page
            yield response.follow(next_page, callback=self.parse)

     # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        # id = response.xpath("//link[@rel='shortlink']/@href").get()

        title = response.css(
            '#l-canvas > div > div > div > div > div.l-page-title > div > h1::text').get()

        address = response.css(
            '#l-canvas > div > div > div > div > main > div.l-container.l-advent--sidebar-right__bottom > div > section.panel-pane.panel-pane--fuse-style.float-none.panel-pane--views.panel-pane--views-panes.panel-pane--property-listing-building-pane-building > div > div > div > div > div > div > ul > li.c-building-card__address-street > div.data__value::text').get()
        city = response.css(
            '#l-canvas > div > div > div > div > main > div.l-container.l-advent--sidebar-right__bottom > div > section.panel-pane.panel-pane--fuse-style.float-none.panel-pane--views.panel-pane--views-panes.panel-pane--property-listing-building-pane-building > div > div > div > div > div > div > ul > li.c-building-card__city > div.data__value::text').get()
        zipcode = response.css(
            '#l-canvas > div > div > div > div > main > div.l-container.l-advent--sidebar-right__bottom > div > section.panel-pane.panel-pane--fuse-style.float-none.panel-pane--views.panel-pane--views-panes.panel-pane--property-listing-building-pane-building > div > div > div > div > div > div > ul > li.c-building-card__address-postal > div.data__value::text').get()

        if address is None:
            address = response.css(
                '#l-canvas > div > div > div > div > div.l-page-title > div > h2::text').get()
            city = address.split(', ')[-3]

        rent = response.css(
            '#l-canvas > div > div > div > div > main > div.l-container.l-wrapper > article > section.panel-pane.panel-pane--fuse-style.float-none.panel-pane--views.panel-pane--views-panes.panel-pane--property-listing-pane-rent > div > div > div > div > div.views-field.views-field-nothing > span > div > span::text').get().split('$')[1]

        description = response.css('#l-canvas > div > div > div > div > main > div.l-container.l-wrapper > article > section.panel-pane.panel-pane--fuse-style.float-none.panel-pane--entity_field.panel-pane--entity-field.panel-pane--nodefield-rental-description.panel-pane--rental-listing > div > div.panel-pane__content > div > div > div > p:nth-child(1)::text').get()

        feats = response.css('#l-canvas > div > div > div > div > main > div.l-container.l-wrapper > article > section.panel-pane.panel-pane--fuse-style.float-none.panel-pane--views.panel-pane--views-panes.panel-pane--property-listing-pane-property-summary > div > div > div > div > div.views-fieldset.views-fieldset-property-information > div.views-fieldset.views-fieldset-property-details > div')

        baths = None
        beds = None
        space = None
        furnished = None
        pets = None
        property_type = "apartment"
        for item in feats:
            if "Bathrooms" in item.css('span.views-label::text').get():
                baths = item.css('span.field-content::text').get()[0]
            elif "Bedrooms" in item.css('span.views-label::text').get():
                beds = item.css('span.field-content::text').get()[0]
            elif "Size" in item.css('span.views-label::text').get():
                space = item.css(
                    'div.field-content::text').get().split(" ")[0]
                space = (int(space)/10.7639)
            elif "Furnished" in item.css('span.views-label::text').get():
                furnished = item.css(
                    'div.field-content::text').get()
                if "Yes" in furnished:
                    furnished = True
                else:
                    furnished = False
            elif "Pets" in item.css('span.views-label::text').get():
                pets = item.css(
                    'span.field-content::text').get()
                if "Yes" in pets:
                    pets = True
                else:
                    pets = False
            elif "Rental Type" in item.css('span.views-label::text').get():
                prop = item.css(
                    'div.field-content::text').get()
                if "house" in prop:
                    property_type = "house"

        images = response.css(
            'div.field__item.even > a > img::attr(src)').extract()

        feats2 = response.css('ul.bullet-list > li')

        dishwasher = None
        washing_machine = None
        pool = None
        parking = None
        for item in feats2:
            if "Dishwasher" in item.css('::text').get():
                dishwasher = True
            elif "Washer" in item.css('::text').get():
                washing_machine = True
            elif "pool" in item.css('::text').get():
                pool = True
            elif "parking" in item.css('::text').get():
                parking = True

        coords = response.xpath('/html/head/script[2]/text()').get()
        lat = coords.split('latitude": "')[1].split('",')[0]
        lng = coords.split('longitude": "')[1].split('"')[0]

        landlord_name = response.css(
            '#l-canvas > div > div > div > div > main > div.l-container.l-wrapper > aside > section.panel-pane.panel-pane--fuse-style.float-none.mobile-visibility-hide.panel-pane--views.panel-pane--views-panes.panel-pane--property-listing-pane-property-manager > div > div > div > div > div.views-field.views-field-nothing.views-fieldset-staff-details > span > div.field--name-field-property-manager-name::text').get()
        landlord_number = response.css(
            '#l-canvas > div > div > div > div > main > div.l-container.l-wrapper > aside > section.panel-pane.panel-pane--fuse-style.float-none.mobile-visibility-hide.panel-pane--views.panel-pane--views-panes.panel-pane--property-listing-pane-property-manager > div > div > div > div > div.views-field.views-field-nothing.views-fieldset-staff-details > span > div.field--name-field-property-manager-cell::text').get()
        landlord_email = response.css(
            '#l-canvas > div > div > div > div > main > div.l-container.l-wrapper > aside > section.panel-pane.panel-pane--fuse-style.float-none.mobile-visibility-hide.panel-pane--views.panel-pane--views-panes.panel-pane--property-listing-pane-property-manager > div > div > div > div > div.views-field.views-field-nothing.views-fieldset-staff-details > span > div.field--name-field-property-manager-email > a::text').get()

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        # item_loader.add_value(
        #     "external_id", "{}".format(id.split("=")[-1].strip()))
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("square_meters", int(int(int(space))*10.764))
        item_loader.add_value("room_count", beds)
        item_loader.add_value("bathroom_count", baths)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("pets_allowed", pets)
        # item_loader.add_value("available_date", avaialble_date)
        item_loader.add_value("parking", parking)
        item_loader.add_value("swimming_pool", pool)
        item_loader.add_value("dishwasher", dishwasher)
        item_loader.add_value("washing_machine", washing_machine)
        # item_loader.add_value("balcony", balcony)

        item_loader.add_value("latitude", lat)
        item_loader.add_value("longitude", lng)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("currency", "CAD")

        # LandLord Details
        item_loader.add_value("landlord_phone", landlord_number)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_name", landlord_name)

        yield item_loader.load_item()
