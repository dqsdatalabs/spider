import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader


class AdvancedpmCaSpider(scrapy.Spider):
    name = 'advancedpm_ca'
    allowed_domains = ['advancedpm.ca']
    start_urls = [
        'https://www.advancedpm.ca/rentals/?location=&price=&submit=Search']
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        for appartment in response.css("body > div.uk-offcanvas-content > div.layout > div > div.uk-width-1-1.uk-width-2-3\@s.uk-width-4-5\@m.main > div.content > div.uk-grid-small"):
            item_loader = ListingLoader(response=response)

            title = appartment.css(
                'div.uk-width-1-1.uk-width-2-3\@m.uk-width-3-4\@l.rentalRight > h3::text').get()

            city = appartment.css(
                'div.uk-width-1-1.uk-width-2-3\@m.uk-width-3-4\@l.rentalRight > h3 > span::text').get()

            address = appartment.css(
                'div.uk-width-1-1.uk-width-2-3\@m.uk-width-3-4\@l.rentalRight > h3 > span::text').get()

            rent = appartment.css('p.rent > strong::text').get().split('$')[1]

            description = response.css(
                'div.uk-width-1-1.uk-width-2-3\@m.uk-width-3-4\@l.rentalRight > p:last-child::text').get()

            property_type = 'apartment'
            try:
                if "home" in description:
                    property_type = 'house'
            except:
                pass

            images = response.css(
                'div.uk-grid-collapse > a::attr(href)').extract()

            some_props = response.css(
                'div.uk-grid-collapse.uk-child-width-1-1.uk-child-width-1-2\@s.info > div > p')

            avaialble_date = None
            rooms = None
            for propaya in some_props:
                if "Availability:" in propaya.css('strong::text').get():
                    avaialble_date = propaya.css('p::text').get()
                elif "Bedrooms:" in propaya.css('strong::text').get():
                    rooms = propaya.css('p::text').get()

            # MetaData
            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            # item_loader.add_value(
            #     "external_id", "{}".format(id.split("=")[-1].strip()))
            item_loader.add_value("title", title)
            item_loader.add_value("description", description)

            # # Property Details
            item_loader.add_value("property_type", property_type)
            # item_loader.add_value("square_meters", int(int(int(space))*10.764))
            item_loader.add_value("room_count", rooms)
            # item_loader.add_value("bathroom_count", bathrooms)
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            # item_loader.add_value("zipcode", zipcode.strip())
            item_loader.add_value("available_date", avaialble_date)
            item_loader.add_value("parking", True)
            item_loader.add_value("pets_allowed", False)
            item_loader.add_value("washing_machine", True)
            # item_loader.add_value("swimming_pool", pool)
            # item_loader.add_value("balcony", balcony)

            # item_loader.add_value("latitude", lat)
            # item_loader.add_value("longitude", lng)

            # Images
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

            # # Monetary Status
            item_loader.add_value("rent", int(rent))
            item_loader.add_value("currency", "CAD")

            # LandLord Details
            item_loader.add_value("landlord_phone", "250-338-2472")
            item_loader.add_value("landlord_email", "info@advancedpm.ca")
            item_loader.add_value(
                "landlord_name", "Advanced Property Management")

            yield item_loader.load_item()
