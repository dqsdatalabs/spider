# -*- coding: utf-8 -*-
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader


class RrimmobiliItSpider(scrapy.Spider):
    name = 'rrimmobili_it'
    allowed_domains = ['rrimmobili.it']
    start_urls = ['https://www.rrimmobili.it/cerca/?filter_search_action%5B%5D=affitto&adv6_search_tab=affitto&term_id=36&adv_location=&filter_search_type%5B%5D=residenziale&is2=1&submit=SEARCH+PROPERTY&adv6_search_tab=affitto&term_id=36']
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        for appartment in response.css("#listing_ajax_container>div"):
            yield Request(appartment.css("div>div::attr(data-link)").get(),
                          callback=self.populate_item,
                          )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css(
            'h1.entry-title.entry-prop::text').get().strip()

        external_id = response.css('link[rel="shortlink"]').attrib['href']
        external_id = "{}".format(external_id.split("=")[-1].strip())

        description = ''
        description_array = response.css("#description > p::text").extract()

        for text in description_array:
            description += text

        images = response.css(
            'img.img-responsive.lightbox_trigger.lazyload::attr(data-src)').extract()


        floor_plans = response.css(
            'img.lightbox_trigger_floor::attr(data-src)').extract()


        lat = response.css("#gmap_wrapper").attrib['data-cur_lat']
        lng = response.css("#gmap_wrapper").attrib['data-cur_long']

        energy_label = response.css(
            "#description > div > div > div > div > div > div:nth-child(7) > div::attr(data-energyclass)").get()

        address_features = response.css("#address>div")

        for item in address_features:
            if "Indirizzo:" in item.css("strong::text").get():
                address = item.css("div::text").get().strip()
                city = address.split(",")[0]

        details = response.css("#details>div")

        balcony = None
        try:
            for item in details:
                if "Bagni:" in item.css("strong::text").get():
                    bathrooms = item.css("div::text").get().strip()
                elif "Camere da letto:" in item.css("strong::text").get():
                    rooms = item.css("div::text").get().strip()
                elif "Posti Auto:" in item.css("strong::text").get():
                    parking = int(item.css("div::text").get().strip())
                    if parking > 0:
                        parking = True
                    else:
                        parking = False
                elif "Prezzo" in item.css("strong::text").get():
                    rent = item.css("div::text").extract()[
                        1].strip().split(" €")[0]
                elif "Balcone" in item.css("div::text").get():
                    balcony = True
                elif "Superficie:" in item.css("strong::text").get():
                    space = item.css(
                        "div::text").get().strip().split(",")[0]

        except:
            pass

        if "." in rent:
            rent_array = rent.split(".")
            rent = rent_array[0]+rent_array[1]
        rent = int(rent)

        if rooms is None:
            return

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)
        item_loader.add_value("energy_label", energy_label)

        # Property Details
        item_loader.add_value("property_type", "apartment")
        item_loader.add_value("square_meters", int(space))
        item_loader.add_value("room_count", rooms)
        item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("parking", parking)
        item_loader.add_value("balcony", balcony)

        item_loader.add_value("latitude", lat)
        item_loader.add_value("longitude", lng)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("floor_plan_images", floor_plans)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        # # LandLord Details
        item_loader.add_value("landlord_phone", "0236728836")
        item_loader.add_value("landlord_email", "info@rrimmobili.it")
        item_loader.add_value("landlord_name", "RR immobili")

        yield item_loader.load_item()
