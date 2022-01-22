import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader


class SvluxuryhouseItSpider(scrapy.Spider):
    name = 'svluxuryhouse_it'
    allowed_domains = ['svluxuryhouse.it']
    start_urls = [
        'https://www.svluxuryhouse.it/properties-search/?status=for-rent&type=residenziale']
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        next_pages = response.css("a.rh_pagination__btn")
        for i in range(len(next_pages)):
            yield Request(next_pages[i].attrib['href'], callback=self.page_follower, dont_filter=True)

    # 1. FOLLOWING
    def page_follower(self, response):
        for appartment in response.css("div.rh_page__listing>article"):
            url = appartment.css(
                "div.rh_list_card__map_details>h3>a").attrib['href']
            yield Request(url, callback=self.populate_item, dont_filter=True)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        external_id = response.css("p.id::text").get()

        title = response.css(
            'body > div.rh_wrap > section.rh_section.rh_wrap--padding.rh_wrap--topPadding > div > div.rh_page__head.rh_page__property > div.rh_page__property_title > h1::text').get()

        property_type = "apartment"
        if 'camera' in title.lower() or 'loft' in title.lower() or 'attico' in title.lower():
            property_type = "room"

        address = None
        try:
            if title.split("ZONA")[1]:
                address = title.split("ZONA ")[1]
        except:
            address = None

        rent = response.css('p.price::text').get().split(" ")[0].split("€")[1]
        if "," in rent:
            rent_array = rent.split(",")
            rent = rent_array[0] + rent_array[1]

        description = ''

        description_array = response.css(
            "div.rh_content>p::text").extract()

        if len(description_array) < 1:
            try:
                description_array = response.css(
                    "div.tab-pane.active>p::text").extract()
            except:
                description_array = None

        if len(description_array) < 1:
            try:
                description_array = response.css(
                    "div.#tab-panel-1 > p::text").extract()
            except:
                description_array = None

        for item in description_array:
            description += item

        description = description.split("Per ulteriori informazioni")[
            0].split("Per maggiori informazioni")[0]

        square_meters = response.css(
            'div.rh_property__row.rh_property__meta_wrap>div:nth-child(3)>div>span::text').get().strip()

        if int(square_meters) < 10:
            square_meters = response.css(
                'div.rh_property__row.rh_property__meta_wrap>div:nth-child(4)>div>span::text').get().strip()

        rooms = response.css(
            'div.rh_property__row.rh_property__meta_wrap>div:nth-child(1)>div>span::text').get()
        bathrooms = response.css(
            'div.rh_property__row.rh_property__meta_wrap>div:nth-child(2)>div>span::text').get()

        images = response.css(
            'a.swipebox>img::attr(src)').extract()

        features = response.css("li.rh_property__feature>a::text").extract()

        elevator = None
        furnished = None
        terrace = None
        balcony = None
        parking = None
        for item in features:
            if item.lower() == 'ascensore':
                elevator = True
            if item.lower() == 'arredato':
                furnished = True
            if item.lower() == 'terrazzo':
                terrace = True
            if item.lower() == 'balcone':
                balcony = True
            if item.lower() == 'box auto':
                parking = True

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", external_id[1:])
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("square_meters", int(square_meters))
        item_loader.add_value("room_count", rooms)
        item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("address", address)
        item_loader.add_value("city", "Rome")

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("currency", "EUR")

        # House Rules
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("parking", parking)

        # LandLord Details
        item_loader.add_value("landlord_phone", "0620294351")
        item_loader.add_value("landlord_email", "info@svluxuryhouse.it")
        item_loader.add_value("landlord_name", "Svluxury House")

        yield item_loader.load_item()
