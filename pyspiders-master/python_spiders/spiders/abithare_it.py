import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader
from scrapy import FormRequest
import json
from ..user_agents import random_user_agent


class AbithareItSpider(scrapy.Spider):
    name = 'abithare_it'
    allowed_domains = ['abithare.it']
    start_urls = [
        'https://www.abithare.it/ricerca/?area&status=immobili-in-affitto&type&bedrooms&bathrooms&min-area&min-price&max-price']
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        next_pages = response.css("ul.pagination>li>a::attr(href)").extract()
        for i in range(len(next_pages)-1):
            yield Request(next_pages[i], callback=self.page_follower)

    # 1. FOLLOWING
    def page_follower(self, response):
        for appartment in response.css("div.property-listing.list-view>div.row>div"):
            url = appartment.css(
                "div.property-item.table-list>div > div > figure > a").attrib['href']
            rooms = appartment.css("span.h-beds::text").get()[-1]
            bathrooms = appartment.css("span.h-baths::text").get()[-1]
            square_meters = appartment.css("span.h-area::text").get()[4:]
            if appartment.css('div.property-item.table-list > div.item-body.table-cell > div.body-left.table-cell > div.info-row.amenities.hide-on-grid > p:nth-child(2) > strong::text').get() == "Appartamento":
                yield Request(url,
                              callback=self.follower,
                              meta={"rooms": rooms,
                                    "bathrooms": bathrooms,
                                    "square_meters": square_meters})

    def follower(self, response):
        id = response.xpath("//link[@rel='shortlink']/@href").get()
        security = response.css("#securityHouzezMap::attr(value)").get()
        formdata = {
            "action": "houzez_get_single_property",
            "prop_id": "{}".format(id.split("=")[-1].strip()),
            "security": security
        }

        yield FormRequest(
            url="https://www.abithare.it/wp-admin/admin-ajax.php",
            callback=self.populate_item,
            formdata=formdata,
            meta={
                "external_id": id,
                "external_link": response.url,
                "porperty": response,
                "rooms": response.meta["rooms"],
                "bathrooms": response.meta["bathrooms"],
                "square_meters": response.meta["square_meters"]
            }
        )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        parsed_response = json.loads(response.body)
        item_loader = ListingLoader(response=response)
        appartment = response.meta["porperty"]

        title = appartment.css('h1::text').get()
        address = appartment.css('address.property-address::text').get()

        property_type = "apartment"

        rent = appartment.css('span.item-price::text').get().split("€")[0]
        if "." in rent:
            rent_array = rent.split(".")
            rent = rent_array[0] + rent_array[1]

        description = ''
        description_array = appartment.css(
            "#description>p::text").extract()

        for text in description_array:
            description += text

        zip_code = appartment.css("li.detail-zip::text").get().strip()
        city = appartment.css("li.detail-city::text").get().strip()

        images = appartment.css(
            'div.item>img::attr(src)').extract()

        elevator = appartment.css("#detail > ul > li:nth-child(1)::text").get()
        if elevator.lower().strip() == "si":
            elevator = True
        else:
            elevator = None

        landlord_name = appartment.css(
            "#agent_bottom > form > div.media.agent-media > div.media-body > dl > dd:nth-child(1)::text").get()

        landlord_number = appartment.css(
            "span.clickToShowPhone::text").get()

        # MetaData
        item_loader.add_value("external_link", response.meta["external_link"])
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value(
            "external_id", "{}".format(response.meta["external_id"].split("=")[-1].strip()))
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # # Property Details
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("square_meters", int(
            response.meta["square_meters"]))
        item_loader.add_value("room_count", response.meta["rooms"])
        item_loader.add_value("bathroom_count", response.meta["bathrooms"])
        item_loader.add_value("address", address)
        item_loader.add_value("zipcode", zip_code)
        item_loader.add_value("city", city)

        item_loader.add_value("latitude", parsed_response["props"][0]["lat"])
        item_loader.add_value("longitude", parsed_response["props"][0]["lng"])

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("currency", "EUR")

        # House Rules
        item_loader.add_value("elevator", elevator)

        # LandLord Details
        item_loader.add_value("landlord_phone", landlord_number)
        item_loader.add_value("landlord_email", "talenti@abithare.it")
        item_loader.add_value("landlord_name", landlord_name)

        yield item_loader.load_item()
