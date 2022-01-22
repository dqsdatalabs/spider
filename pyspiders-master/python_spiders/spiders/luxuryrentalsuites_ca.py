import re
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader


class LuxuryRentalSuitesSpider(scrapy.Spider):
    name = 'luxuryrentalsuites_ca'
    allowed_domains = ['luxuryrentalsuites.ca']
    start_urls = [
        'https://luxuryrentalsuites.ca/lrs/advanced-search/?filter_search_action%5B%5D=furnished&adv6_search_tab=furnished&term_id=9&filter_search_type%5B%5D=&advanced_city=&advanced_area=&property-id=&bedrooms=&bathrooms=&pets-allowed=&price_low_9=1500&price_max_9=30000&available-date=&submit=Search+Properties']
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def start_request(self, response):
        yield Request("https://luxuryrentalsuites.ca/lrs/advanced-search/?filter_search_action%5B%5D=furnished&adv6_search_tab=furnished&term_id=9&filter_search_type%5B%5D=&advanced_city=&advanced_area=&property-id=&bedrooms=&bathrooms=&pets-allowed=&price_low_9=1500&price_max_9=30000&available-date=&submit=Search+Properties",
                      method="GET",
                      callback=self.parse,
                      body='',
                      )

    def parse(self, response):
        for appartment in response.css("#listing_ajax_container > div"):
            url = appartment.css(
                "div.property_listing > h4 > a").attrib['href']
            yield Request(url,
                          callback=self.populate_item,
                          )

        next_page = response.css(
            '#google_map_prop_list_sidebar > div.half-pagination > ul > li.roundright > a').attrib['href']
        if next_page:
            yield Request(
                url=next_page,
                callback=self.parse,
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        id = response.xpath("//link[@rel='shortlink']/@href").get()

        title = response.css(
            '#all_wrapper > div > div.container.content_wrapper > div > div.col-xs-12.col-md-9.rightmargin.full_width_prop > div.single-content.page_template_loader > div.vc_row.wpb_row.vc_row-fluid.vc_row-o-equal-height.vc_row-o-content-middle.vc_row-flex.vc_row > div.wpb_column.vc_column_container.vc_col-sm-9.vc_column > div > div > div > h1::text').get()

        address_features = response.css(
            'div.panel-collapse.collapse.in > div > div')

        address = None
        city = None
        zipcode = None
        bedrooms = None
        avaialble_date = None
        bathrooms = None
        parking = None
        pets = None
        space = None
        balcony = None
        pool = None
        Dishwasher = None
        for item in address_features:
            if item.css("strong::text"):
                if "Address" in item.css("strong::text").get():
                    address = item.css('div::text').get()
                elif "City:" in item.css("strong::text").get():
                    city = item.css('a::text').get()
                elif "Zip/Postal Code" in item.css("strong::text").get():
                    zipcode = item.css('div::text').get()
                elif "Bathrooms" in item.css("strong::text").get():
                    bathrooms = item.css('div::text').get()
                elif "Bedrooms" in item.css("strong::text").get():
                    bedrooms = item.css('div::text').get()
                elif "Available Date" in item.css("strong::text").get():
                    avaialble_date = item.css('div::text').get()
                elif "# Of Parking:" in item.css("strong::text").get():
                    parking = item.css('div::text').get().strip()
                    try:
                        parking = int(parking)
                        if parking > 0:
                            parking = True
                        else:
                            parking = False
                    except:
                        parking = None
                elif "Pets Allowed?" in item.css("strong::text").get():
                    pets = item.css('div::text').get().strip()
                    try:
                        if "Yes" in pets:
                            pets = True
                        else:
                            pets = False
                    except:
                        pets = None
                elif "Price" in item.css("strong::text").get():
                    rent = item.css('div::text').extract()[
                        1].strip().split(" ")[1]
                    try:
                        if "," in rent:
                            rent = rent.split(",")
                            rent = rent[0]+rent[1]
                    except:
                        rent = rent
                elif "Property Size" in item.css("strong::text").get():
                    space = item.css('div::text').get().strip().split(" ")[0]
                    if "," in space:
                        space = space.split(",")
                        space = space[0]+space[1]
                    space = int(space)/10.7639
            else:
                if "Balcony" in item.css("::text").get():
                    balcony = True
                elif "Swimming Pool" in item.css("::text").get():
                    pool = True
                elif "Dishwasher" in item.css("::text").get():
                    Dishwasher = True

        description = ''
        description_array = response.css(
            "#all_wrapper > div > div.container.content_wrapper > div > div.col-xs-12.col-md-9.rightmargin.full_width_prop > div.single-content.page_template_loader > div:nth-child(6) > div > div > div > div.wpestate_estate_property_details_section > p::text").extract()

        for item in description_array:
            description += item

        images = response.css(
            '#carousel-property-page-header > div.carousel-inner > div.item > div.propery_listing_main_image.lightbox_trigger::attr(style)').extract()

        for i in range(len(images)):
            images[i] = images[i].split(':url(')[1].split(")")[0]

        coords = response.xpath(
            '//*[@id="all_wrapper"]/div/script[18]/text()').get()
        lat = coords.split('general_latitude":"')[1].split('",')[0]
        lng = coords.split('longitude":"')[1].split('",')[0]

        landlord_name = response.css(
            '#primary > div > div.agent_unit > div:nth-child(2) > h4 > a::text').get()
        landlord_number = response.css(
            '#primary > div > div.agent_unit > div:nth-child(2) > div:nth-child(3)::text').get()
        landlord_email = response.css(
            '#primary > div > div.agent_unit > div:nth-child(2) > div:nth-child(4)::text').get()

        if bedrooms:
            bedrooms = int(bedrooms[1])

        if bathrooms:
            bathrooms = int(bathrooms[1])

        if bedrooms == 0:
            bedrooms = 1

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value(
            "external_id", "{}".format(id.split("=")[-1].strip()))
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", "apartment")
        item_loader.add_value("square_meters", int(int(int(space))*10.764))
        item_loader.add_value("room_count", bedrooms)
        item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode.strip())
        item_loader.add_value("available_date", avaialble_date)
        item_loader.add_value("parking", parking)
        item_loader.add_value("swimming_pool", pool)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("dishwasher", Dishwasher)
        item_loader.add_value("pets_allowed", pets)

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
