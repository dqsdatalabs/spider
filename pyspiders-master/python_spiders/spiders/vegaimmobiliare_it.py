# -*- coding: utf-8 -*-
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader


class VegaimmobiliareItSpider(scrapy.Spider):
    name = 'vegaimmobiliare_it'
    allowed_domains = ['vegaimmobiliare.it']
    start_urls = [
        'https://www.vegaimmobiliare.it/property-status/affitti-residenziali/']
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        for appartment in response.css("body > div.qodef-wrapper > div > div.qodef-content > div > div > div > div > div.qodef-property-list-items-part > div.qodef-pl-inner.qodef-outer-space.qodef-ml-inner.clearfix>article"):
            url = appartment.css(
                "div.qodef-pl-item-inner>a.qodef-pli-link.qodef-block-drag-link").attrib['href']
            yield Request(url,
                          callback=self.populate_item,
                          dont_filter=True,
                          )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        id = response.xpath('/html/head/link[36]/@href').get()

        title = response.css('h2.qodef-title-title::text').get().strip()
        address = response.css("body > div.qodef-wrapper > div.qodef-wrapper-inner > div.qodef-content > div > div > div > div > div.qodef-container > div > div > div.qodef-page-content-holder.qodef-grid-col-9 > div > div.qodef-property-map.qodef-property-label-items-holder > div.qodef-property-map-items.qodef-property-items-style.clearfix > div.qodef-property-map-address > div > div:nth-child(1) > span > span.qodef-label-items-value::text").get().strip()
        city = address.split(',')[1]

        zipcode = response.css("body > div.qodef-wrapper > div.qodef-wrapper-inner > div.qodef-content > div > div > div > div > div.qodef-container > div > div > div.qodef-page-content-holder.qodef-grid-col-9 > div > div.qodef-property-map.qodef-property-label-items-holder > div.qodef-property-map-items.qodef-property-items-style.clearfix > div.qodef-property-map-address > div > div:nth-child(3) > span > span.qodef-label-items-value::text").get().strip()
        rent = response.css(
            'div.qodef-property-price>span>span.qodef-property-price-value::text').get().strip()

        if "." in rent:
            rent_array = rent.split(".")
            rent = rent_array[0] + rent_array[1]

        description = ''
        description_array = response.css(
            "div.qodef-property-description.qodef-property-label-items-holder > div.qodef-property-description-items.qodef-property-items-style.clearfix>p::text").extract()

        for text in description_array:
            description += text

        images = response.css(
            'a.qodef-property-single-lightbox>img::attr(src)').extract()

        floor_plan_images = response.css(
            "#ui-id-2 > div > div.qodef-accordion-image > img::attr(src)").extract()

        if len(images) < 2:
            return

        features = response.css(
            "div.qodef-property-spec-items.qodef-property-items-style.clearfix>div.qodef-spec>div>div")

        rooms = None
        bathrooms = None
        space = None
        floor = None
        for item in features:
            try:
                if "Vani:" in item.css("span.qodef-label-text::text").get():
                    rooms = item.css(
                        "span.qodef-spec-item-value.qodef-label-items-value::text").get().strip()
                elif "Mq:" in item.css("span.qodef-label-text::text").get():
                    space = item.css(
                        "span.qodef-spec-item-value.qodef-label-items-value::text").get().strip()
                elif "Bagni:" in item.css("span.qodef-label-text::text").get():
                    bathrooms = item.css(
                        "span.qodef-spec-item-value.qodef-label-items-value::text").get().strip()
                elif "Piano:" in item.css("span.qodef-label-text::text").get():
                    floor = item.css(
                        "span.qodef-spec-item-value.qodef-label-items-value::text").get().strip()
            except:
                pass

        features_2 = response.css("div.qodef-feature.qodef-feature-active")

        elevator = None
        balcony = None
        swimming_pool = None
        terrace = None
        parking = None
        for item in features_2:
            if "Ascensore" in item.css("span.qodef-feature-name::text").get().strip():
                elevator = True
            elif "BALCONE" in item.css("span.qodef-feature-name::text").get().strip().upper():
                balcony = True
            elif "Box/posti auto" in item.css("span.qodef-feature-name::text").get().strip():
                parking = True
            elif "Terrazzo" in item.css("span.qodef-feature-name::text").get().strip():
                terrace = True
            elif "Piscina" in item.css("span.qodef-feature-name::text").get().strip():
                swimming_pool = True

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", id.split('=')[1])
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", "apartment")
        item_loader.add_value("square_meters", int(space))
        item_loader.add_value("room_count", rooms)
        item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("floor", floor)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("floor_plan_images", floor_plan_images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("currency", "EUR")

        # Features
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("swimming_pool", swimming_pool)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("parking", parking)

        # LandLord Details
        item_loader.add_value("landlord_phone", "390817611166")
        item_loader.add_value("landlord_email", "info@vegaimmobiliare.it")
        item_loader.add_value("landlord_name", "Vega Immobiliare")

        yield item_loader.load_item()
