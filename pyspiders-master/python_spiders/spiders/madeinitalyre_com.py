import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader


class MadeinitalyreComSpider(scrapy.Spider):
    name = 'madeinitalyre_com'
    allowed_domains = ['madeinitalyre.com']
    start_urls = [
        'http://www.madeinitalyre.com/lista-proprieta-residenziali-in-affitto/']
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        for appartment in response.css('#listing_ajax_container>div'):
            if "Appartamento" in appartment.css("div.info_container_unit_3 > div.property_categ_unit_type3 > span > a::text").extract():
                yield Request(appartment.css("div.listing-unit-img-wrapper>a").attrib['href'],
                              callback=self.populate_item,
                              dont_filter=True)

        try:
            next_page = response.css('li.roundright > a').attrib['href']
        except:
            next_page = None

        if next_page is not None:
            yield response.follow(next_page, callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css('h1.entry-title.entry-prop::text').get()

        address_text = ""
        city = ""
        address = response.css('span.adres_area>a::text').extract()
        for i in range(len(address)):
            if address[i] != address[-1]:
                address_text += address[i]
                address_text += ", "
                city += address[i]
                city += ", "
            else:
                address_text += address[i]
                city = city[:-2]

        rent = response.css('span.price_area::text').get().split(" ")[1]
        if "." in rent:
            rent_array = rent.split(".")
            rent = rent_array[0] + rent_array[1]

        description = response.css(
            "div.wpestate_property_description>p::text").get()

        square_meters = None
        rooms = None
        floor = None
        bathrooms = None
        utilities = None
        for item in response.css('#collapseOne>div>div'):
            if item.css('strong::text').get() == "Superficie:":
                square_meters = item.css("div::text").get().split(" ")[1]
            elif item.css('strong::text').get() == "Vani:":
                rooms = item.css("div::text").get()
            elif item.css('strong::text').get() == "Piano:":
                floor = item.css("div::text").get().split(" ")[1]
            elif item.css('strong::text').get() == "Bagni:":
                bathrooms = item.css("div::text").get()

        features = response.css(
            "#collapseOne > div > div")

        external_id = None
        furnished = None
        for item in features:
            if "Riferimento:" in item.css("strong::text").get():
                external_id = item.css("div::text").get().strip()
            elif "Arredato:" in item.css("strong::text").get():
                furnished = item.css("div::text").get().strip()
                if "SI" in furnished.strip():
                    furnished = True
            elif "Spese Condominiali:" in item.css('strong::text').get():
                utilities = item.css("div::text").get().strip().split(" ")[1]

        images = response.css(
            'div.item>a>img::attr(src)').extract()

        landlord_name = response.css(
            "#primary > div > div.agent_unit > div:nth-child(2) > h4 > a::text").get()
        landlord_phone = response.css(
            "#primary > div > div.agent_unit > div:nth-child(2) > div:nth-child(3)>a::text").get()

        lat = response.xpath('//*[@id="gmap_wrapper"]/@data-cur_lat').get()
        long = response.xpath('//*[@id="gmap_wrapper"]/@data-cur_long').get()

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", 'apartment')
        item_loader.add_value("square_meters", int(square_meters))
        item_loader.add_value("room_count", rooms)
        item_loader.add_value("floor", floor)
        item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("address", address_text)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("city", city)

        item_loader.add_value("latitude", lat)
        item_loader.add_value("longitude", long)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("currency", "EUR")

        # LandLord Details
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", "info@madeinitalyre.com")
        item_loader.add_value("landlord_name", landlord_name)

        yield item_loader.load_item()
